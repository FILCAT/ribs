package logcache

import (
	"encoding/binary"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
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

// LogCache is a cache of logs, backed by a car file and an index file
// The car file contains a dummy header, and a sequence of blocks stored with raw cids
// the index is in-memory + on disk, contains a set of dataLen/offset/multihash entries
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
	lc.carFile, err = os.OpenFile(filepath.Join(dir, base+carExt), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, xerrors.Errorf("open cache car: %w", err)
	}
	lc.carBuf = lc.carFile

	st, err := lc.carFile.Stat()
	if err != nil {
		return nil, xerrors.Errorf("stat cache car: %w", err)
	}

	if st.Size() == 0 {
		// new car, write a dummy header

		head := &car.CarHeader{
			Roots:   nil,
			Version: 1,
		}

		err = car.WriteHeader(head, lc.carFile)
		if err != nil {
			return nil, xerrors.Errorf("write cache car header: %w", err)
		}

		st, err = lc.carFile.Stat()
		if err != nil {
			return nil, xerrors.Errorf("stat cache car: %w", err)
		}
	}

	lc.carAt = st.Size()

	// open index file
	lc.indexFile, err = os.OpenFile(filepath.Join(dir, base+idxExt), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, xerrors.Errorf("open cache index: %w", err)
	}
	lc.idxBuf = lc.indexFile

	var entBuf [512]byte
	var lastOffLen uint64

	// load index file into index
	for {
		// read the 9 entry bytes
		n, err := lc.indexFile.Read(entBuf[:9])
		if err != nil {
			if err == io.EOF && n == 0 {
				break
			}

			// todo handle truncated index
			return nil, xerrors.Errorf("reading index header entry: %w", err)
		}

		lastOffLen = binary.LittleEndian.Uint64(entBuf[:8])

		hlen := entBuf[8]

		// read hash
		n, err = lc.indexFile.Read(entBuf[:hlen])
		if err != nil {
			return nil, xerrors.Errorf("read index entry key: %w", err)
		}

		lc.index[mhStr(entBuf[:hlen])] = lastOffLen
	}

	if lastOffLen > 0 { // first entry is never at 0 because that's where the car header is
		offset, length := carlog.FromOffsetLen(int64(lastOffLen))

		expectedLen := offset + int64(length) + int64(binary.PutUvarint(entBuf[:], uint64(length)))
		if st.Size() != expectedLen {
			// todo truncate car file (or the index.. or both)
			return nil, xerrors.Errorf("car file is truncated, expected %d bytes, got %d", expectedLen, st.Size())
		}
	}

	_, err = lc.carFile.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, xerrors.Errorf("seek to end of car file: %w", err)
	}

	_, err = lc.indexFile.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, xerrors.Errorf("seek to end of index file: %w", err)
	}

	return lc, nil
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
	idxBuf[8] = byte(len(mh))
	n := copy(idxBuf[9:], mh)
	if n != int(idxBuf[8]) {
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
	vlen := binary.PutUvarint(buf, uint64(length))

	_, err := lc.carBuf.ReadAt(buf[:length+vlen], off)
	if err != nil {
		return xerrors.Errorf("read cache car data: %w", err)
	}

	// buf is [len:varint][cid][data]

	// read length
	n, l := binary.Uvarint(buf) // todo skip this
	if uint64(length) != n {
		return xerrors.Errorf("index entry len mismatch")
	}

	cl, _, err := cid.CidFromBytes(buf[l:]) // todo more optimized skip read
	if err != nil {
		return xerrors.Errorf("read cache car cid: %w", err)
	}

	return cb(buf[cl+l : l+int(n)])
}

func (lc *LogCache) Flush() error {
	lc.writeLk.Lock()
	defer lc.writeLk.Unlock()

	// todo sync idx / car Bufs once those are actually buffers

	if err := lc.carFile.Sync(); err != nil {
		return xerrors.Errorf("sync car: %w", err)
	}

	if err := lc.indexFile.Sync(); err != nil {
		return xerrors.Errorf("sync index: %w", err)
	}

	return nil
}

// Close closes the opened car and index files.
func (lc *LogCache) Close() error {
	lc.writeLk.Lock()
	defer lc.writeLk.Unlock()

	// todo sync idx / car Bufs once those are actually buffers

	if err := lc.carFile.Close(); err != nil {
		return xerrors.Errorf("close car file: %w", err)
	}

	if err := lc.indexFile.Close(); err != nil {
		return xerrors.Errorf("close index file: %w", err)
	}

	return nil
}
