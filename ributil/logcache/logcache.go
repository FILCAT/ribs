package logcache

import (
	"encoding/binary"
	"github.com/ipfs/go-cid"
	"github.com/lotus-web3/ribs/carlog"
	"golang.org/x/xerrors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/multiformats/go-multihash"
)

const (
	carExt = ".car"
	idxExt = ".offs"
)

type mhStr string

type ReaderAtWriter interface {
	io.ReaderAt
	io.Writer
}

type LogCache struct {
	carFile *os.File

	// index file
	// [[i24:dataLen][i48:offset]:i64][mhLen:i8][multihash]...
	indexFile *os.File

	// todo use some peekable buffered writer for this
	carBuf, idxBuf ReaderAtWriter
	carAt          int64

	indexLk sync.Mutex
	index   map[mhStr]uint64

	// entirely separate write mutex because reads happen on read-only data
	readLk  sync.RWMutex
	writeLk sync.Mutex
}

func Open(basePath string) (lc *LogCache, err error) {
	lc = &LogCache{
		index: map[mhStr]uint64{},
	}

	dir, base := filepath.Split(basePath)

	// open car file
	lc.carFile, err = os.Open(filepath.Join(dir, base+carExt))
	if err != nil {
		return nil, xerrors.Errorf("open cache car: %w", err)
	}

	// todo if this is a newly created car, write a dummy header

	lc.carAt

	// open index file
	idxFile, err := os.Open(filepath.Join(dir, base+idxExt))
	if err != nil {
		return nil, xerrors.Errorf("open cache index: %w", err)
	}
	lc.idxBuf = idxFile

	var entBuf [512]byte

	// load index file into index
	for {
		// read the 9 header bytes
		n, err := idxFile.Read(entBuf[:9])
		if err != nil {
			if err == io.EOF && n == 0 {
				break
			}

			// todo handle truncated index
			return nil, xerrors.Errorf("reading index header entry: %w", err)
		}

		offLen := binary.LittleEndian.Uint64(entBuf[:8])

		hlen := entBuf[8]

		// read hash
		n, err = idxFile.Read(entBuf[:hlen])
		if err != nil {
			return nil, xerrors.Errorf("read index entry key: %w", err)
		}

		lc.index[mhStr(entBuf[:hlen])] = offLen
	}

	// check that last index file points to a valid car entry, and that there's no
	// data at the end of carFile
	// - if there is trailer data in the car, truncate it
	// - if the index points beyond the end of the car file, scan the index for the
	//   last entry which is within the car, truncate the index, then read the car (if last entry is truncated, do truncate it)
}

// Put returns an error if the multihash is already stored in the cache, otherwise
// it appends the entry to the cache car, then appends an index entry
func (lc *LogCache) Put(mh multihash.Multihash, data []byte) (err error) {
	lc.writeLk.Lock()
	defer lc.writeLk.Unlock()

	bcid := cid.NewCidV1(cid.Raw, mh).Bytes()

	wrote, err := lc.ldWrite(bcid, data)
	if err != nil {
		return err
	}

	offLen := uint64(carlog.MakeOffsetLen(lc.carAt, int(wrote)))
	lc.carAt += wrote

	lc.readLk.Lock()
	lc.index[mhStr(mh)] = offLen
	lc.readLk.Unlock()

	var idxBuf [512]byte
	binary.LittleEndian.PutUint64(idxBuf[:8], offLen)
	idxBuf[9] = byte(len(mh))
	n := copy(idxBuf[9:], mh)
	if n != int(idxBuf[9]) {
		return xerrors.Errorf("copied unexpected number of bytes when writing cache entry multihash; max hash len is 255 bytes")
	}

	_, err = lc.idxBuf.Write(idxBuf[:n+9])
	if err != nil {
		return xerrors.Errorf("writing cache index entry: %w", err)
	}

	return nil
}

func (lc *LogCache) ldWrite(d ...[]byte) (int64, error) {
	var sum uint64
	for _, s := range d {
		sum += uint64(len(s))
	}

	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, sum)

	_, err := lc.carBuf.Write(buf[:n])
	if err != nil {
		// todo flag as corrupt
		return 0, err
	}

	for _, s := range d {
		_, err = lc.carBuf.Write(s)
		if err != nil {
			// todo flag as corrupt
			return 0, err
		}
	}

	return int64(sum), nil
}

func (lc *LogCache) Get(mh multihash.Multihash, cb func([]byte) error) error {
	lc.readLk.RLock()
	atOffLen, found := lc.index[mhStr(mh)]
	lc.readLk.RUnlock()

	if !found {
		return nil
	}

	off, length := carlog.FromOffsetLen(int64(atOffLen))

	buf := make([]byte, length+binary.MaxVarintLen64) // todo pool!

	_, err := lc.carBuf.ReadAt(buf, off)
	if err != nil {
		return xerrors.Errorf("read cache car data: %w", err)
	}
}

func (lc *LogCache) Flush() error {

}
