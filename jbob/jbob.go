package jbob

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	blocks "github.com/ipfs/go-block-format"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/lotus-web3/ribs/bsst"
	"io"
	"math/bits"
	"os"
	"path/filepath"

	"golang.org/x/xerrors"

	mh "github.com/multiformats/go-multihash"
)

const (
	HeadName = "head"
	HeadSize = 512

	LevelIndex = "index.level"
	BsstIndex  = "index.bsst"
)

const jbobBufSize = 16 << 20

// JBOB stands for "Just A Bunch Of Blocks"
// * NOT THREAD SAFE FOR WRITING!!
// * One tx at a time
// * Not considered written until committed
type JBOB struct {
	// index = dir, data = file
	IndexPath, DataPath string

	// head is a file which contains cbor-map-serialized Head, padded up to head
	// size
	head *os.File

	// data contains a log of all written data
	// [[len: u4][logEntryType: u8][data]]..
	data *os.File

	dataBuffered *bufio.Writer

	// current data file length
	dataLen int64

	// index

	wIdx WritableIndex
	rIdx ReadableIndex

	// buffers
	headBuf [HeadSize]byte
}

// Head is the on-disk head object. CBOR-map-serialized. Must fit in
//
//	HeadSize bytes. Null-Padded to exactly HeadSize
type Head struct {
	// todo version

	// something that's not zero
	Valid bool

	// byte offset just after the last retired op
	RetiredAt int64

	ReadOnly  bool // if true, no more writes are allowed
	Finalized bool // if true, no more writes are allowed, and the bsst index is finalized

	// todo entry count
}

type logEntryType byte

const (
	entInvalid logEntryType = iota

	// entBlock data is encoded as \0[mhlen: u2][data][multihash]
	entBlock
)

func Create(indexPath, dataPath string) (*JBOB, error) {
	if err := os.Mkdir(indexPath, 0755); err != nil {
		return nil, xerrors.Errorf("mkdir index path (%s): %w", indexPath, err)
	}

	headFile, err := os.OpenFile(filepath.Join(indexPath, HeadName), os.O_RDWR|os.O_SYNC|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return nil, xerrors.Errorf("opening head: %w", err)
	}

	dataFile, err := os.OpenFile(filepath.Join(dataPath), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return nil, xerrors.Errorf("opening head: %w", err)
	}

	h := &Head{
		Valid:     true,
		RetiredAt: 0,
	}
	var headBuf [HeadSize]byte

	if err := h.MarshalCBOR(bytes.NewBuffer(headBuf[:0])); err != nil {
		return nil, xerrors.Errorf("set head: %w", err)
	}

	n, err := headFile.WriteAt(headBuf[:], 0)
	if err != nil {
		return nil, xerrors.Errorf("HEAD WRITE ERROR (new head: %x): %w", headBuf[:], err)
	}
	if n != len(headBuf) {
		return nil, xerrors.Errorf("bad head written bytes (%d bytes, new head: %x)", n, headBuf[:])
	}

	if err := headFile.Sync(); err != nil {
		return nil, xerrors.Errorf("head sync (new head: %x): %w", headBuf[:], err)
	}

	// new index is always level

	idx, err := OpenLevelDBIndex(filepath.Join(indexPath, LevelIndex), true)
	if err != nil {
		return nil, xerrors.Errorf("creating leveldb index: %w", err)
	}

	return &JBOB{
		IndexPath:    indexPath,
		DataPath:     dataPath,
		head:         headFile,
		data:         dataFile,
		dataBuffered: bufio.NewWriterSize(dataFile, jbobBufSize),
		dataLen:      0,

		wIdx: idx,
		rIdx: idx,
	}, nil
}

func Open(indexPath, dataPath string) (*JBOB, error) {
	headFile, err := os.OpenFile(filepath.Join(indexPath, HeadName), os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		return nil, xerrors.Errorf("opening head: %w", err)
	}

	// read head
	var headBuf [HeadSize]byte
	n, err := headFile.ReadAt(headBuf[:], 0)
	if err != nil {
		return nil, xerrors.Errorf("HEAD READ ERROR: %w", err)
	}
	if n != len(headBuf) {
		return nil, xerrors.Errorf("bad head read bytes (%d bytes)", n)
	}

	var h Head
	if err := h.UnmarshalCBOR(bytes.NewBuffer(headBuf[:])); err != nil {
		return nil, xerrors.Errorf("unmarshal head: %w", err)
	}

	// open data
	dataFile, err := os.OpenFile(dataPath, os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		return nil, xerrors.Errorf("opening data: %w", err)
	}

	// check if data needs to be replayed/truncated
	dataInfo, err := dataFile.Stat()
	if err != nil {
		return nil, xerrors.Errorf("stat data len: %w", err)
	}

	if dataInfo.Size() > h.RetiredAt { // data ahead means there was an unclean shutdown during a write
		// todo truncate data / replay

		return nil, xerrors.Errorf("data file is longer than head says it should be (%d > %d, by %d B)", dataInfo.Size(), h.RetiredAt, dataInfo.Size()-h.RetiredAt)
	}

	if dataInfo.Size() < h.RetiredAt {
		// something is not yes

		return nil, xerrors.Errorf("data file is shorter than head says it should be (%d < %d)", dataInfo.Size(), h.RetiredAt)
	}

	// seek to data end as writes are appended
	if _, err := dataFile.Seek(dataInfo.Size(), io.SeekStart); err != nil {
		return nil, xerrors.Errorf("seeking to data end: %w", err)
	}

	jb := &JBOB{
		IndexPath:    indexPath,
		DataPath:     dataPath,
		head:         headFile,
		data:         dataFile,
		dataBuffered: bufio.NewWriterSize(dataFile, jbobBufSize),
		dataLen:      dataInfo.Size(),
	}

	// open index
	if h.Finalized {
		// bsst, read only
		idx, err := OpenBSSTIndex(filepath.Join(indexPath, BsstIndex))
		if err != nil {
			return nil, xerrors.Errorf("opening bsst index: %w", err)
		}

		jb.rIdx = idx
	} else {
		idx, err := OpenLevelDBIndex(filepath.Join(indexPath, LevelIndex), false)
		if err != nil {
			return nil, xerrors.Errorf("opening leveldb index: %w", err)
		}

		jb.rIdx = idx

		if h.ReadOnly {
			// todo start finalize
			//  (this should happen through group mgr)
		} else {
			jb.wIdx = idx
		}
	}

	return jb, nil
}

/* WRITE SIDE */

type WritableIndex interface {
	// Put records entries in the index
	// sync for now, todo
	// -1 offset means 'skip'
	Put(c []mh.Multihash, offs []int64) error
	//Del(c []mh.Multihash, offs []int64) error

	// todo Sync() error

	Close() error
}

type ReadableIndex interface {
	// todo maybe callback calling with sequential indexes of what we don't have
	//  to pipeline better?
	Has(c []mh.Multihash) ([]bool, error)

	// Get returns offsets to data, -1 if not found
	Get(c []mh.Multihash) ([]int64, error)

	// bsst creation
	Entries() (int64, error)
	bsst.Source

	Close() error
}

func (j *JBOB) mutHead(mut func(h *Head) error) error {
	// todo cache current
	n, err := j.head.ReadAt(j.headBuf[:], 0)
	if err != nil {
		return xerrors.Errorf("read head: %w", err)
	}
	if n != len(j.headBuf) {
		return xerrors.Errorf("head mis-sized (%d bytes)", n)
	}

	var h Head
	if err := h.UnmarshalCBOR(bytes.NewReader(j.headBuf[:])); err != nil {
		return xerrors.Errorf("unmarshalling head: %w", err)
	}

	if !h.Valid {
		return xerrors.Errorf("stored head invalid")
	}

	if err := mut(&h); err != nil {
		return err
	}

	for i := range j.headBuf {
		j.headBuf[i] = 0
	}

	if err := h.MarshalCBOR(bytes.NewBuffer(j.headBuf[:0])); err != nil {
		return xerrors.Errorf("set head: %w", err)
	}

	n, err = j.head.WriteAt(j.headBuf[:], 0)
	if err != nil {
		return xerrors.Errorf("HEAD WRITE ERROR (new head: %x): %w", j.headBuf[:], err)
	}
	if n != len(j.headBuf) {
		return xerrors.Errorf("bad head written bytes (%d bytes, new head: %x)", n, j.headBuf[:])
	}

	if err := j.head.Sync(); err != nil {
		return xerrors.Errorf("head sync (new head: %x): %w", j.headBuf[:], err)
	}

	return nil
}

func (j *JBOB) Put(c []mh.Multihash, b []blocks.Block) error {
	if j.wIdx == nil {
		return xerrors.Errorf("cannot write to read-only jbob")
	}

	if len(c) != len(b) {
		return xerrors.Errorf("hash list length doesn't match blocks length")
	}
	offsets := make([]int64, len(b))

	entHead := []byte{0, 0, 0, 0, byte(entBlock), 0, 0, 0}

	hasList, err := j.rIdx.Has(c)
	if err != nil {
		return err
	}

	// todo optimize (at least buffer) writes

	// first append to log
	for i, blk := range b {
		if hasList[i] {
			offsets[i] = -1
			continue
		}

		offsets[i] = j.dataLen
		data := blk.RawData()

		binary.LittleEndian.PutUint32(entHead, 1+2+uint32(len(data))+uint32(len(c[i])))
		binary.LittleEndian.PutUint16(entHead[6:], uint16(len(c[i])))
		if _, err := j.dataBuffered.Write(entHead); err != nil {
			return xerrors.Errorf("writing entry header: %w", err)
		}

		if _, err := j.dataBuffered.Write(data); err != nil {
			return xerrors.Errorf("writing entry header: %w", err)
		}

		// todo separate 'unhashed' block type for small blocks
		if _, err := j.dataBuffered.Write(c[i]); err != nil {
			return xerrors.Errorf("writing entry header: %w", err)
		}

		j.dataLen += int64(len(data)) + int64(len(entHead)) + int64(len(c[i]))
	}

	// log the write
	// todo async

	if err := j.wIdx.Put(c, offsets); err != nil {
		return xerrors.Errorf("updating index: %w", err)
	}

	return nil
}

var errNothingToCommit = errors.New("nothing to commit")

func (j *JBOB) Commit() (int64, error) {
	// todo log commit?

	var err error
	for {
		err = j.dataBuffered.Flush()
		if err != io.ErrShortWrite {
			break
		}
	}

	if err != nil {
		return 0, xerrors.Errorf("flushing buffered data: %w", err)
	}

	if err := j.data.Sync(); err != nil {
		return 0, xerrors.Errorf("sync data: %w", err)
	}

	// todo index is sync for now, and we're single threaded, so if there were any
	// puts, just update head

	err = j.mutHead(func(h *Head) error {
		if h.RetiredAt == j.dataLen {
			return errNothingToCommit
		}

		h.RetiredAt = j.dataLen
		return nil
	})
	switch err {
	case nil:
		return j.dataLen, nil
	case errNothingToCommit:
		return j.dataLen, nil
	default:
		return 0, xerrors.Errorf("mutate head: %w", err)
	}
}

/* READ SIDE */

func (j *JBOB) View(c []mh.Multihash, cb func(cidx int, found bool, data []byte) error) error {
	locs, err := j.rIdx.Get(c)
	if err != nil {
		return xerrors.Errorf("getting value locations: %w", err)
	}

	if j.dataBuffered.Buffered() > 0 {
		for {
			err = j.dataBuffered.Flush()
			if err != io.ErrShortWrite {
				break
			}
		}

		if err != nil {
			return xerrors.Errorf("flushing buffered data: %w", err)
		}
	}

	entBuf := pool.Get(1 << 20)
	defer pool.Put(entBuf)

	for i := range c {
		if locs[i] == -1 {
			if err := cb(i, false, nil); err != nil {
				return err
			}
			continue
		}

		// todo: optimization: keep len in index
		var entHead [8]byte
		if _, err := j.data.ReadAt(entHead[:], locs[i]); err != nil {
			return xerrors.Errorf("reading entry header: %w", err)
		}
		entType := entHead[4]
		if entType != byte(entBlock) {
			return xerrors.Errorf("unexpected entry type %d, expected block (1)", entType)
		}
		mhLen := uint32(binary.LittleEndian.Uint16(entHead[6:]))

		entLen := binary.LittleEndian.Uint32(entHead[:4]) - 1 - 2 - mhLen
		if entLen > uint32(len(entBuf)) {
			// expand buffer to next power of two if needed
			pool.Put(entBuf)
			entBuf = pool.Get(1 << bits.Len32(entLen))
		}

		if _, err := j.data.ReadAt(entBuf[:entLen], locs[i]+int64(len(entHead))); err != nil {
			return xerrors.Errorf("reading entry: %w", err)
		}

		if err := cb(i, true, entBuf[:entLen]); err != nil {
			return err
		}
	}

	return nil
}

var ErrNotReadOnly = errors.New("not yet read-only")

func (j *JBOB) Iterate(cb func(c mh.Multihash, data []byte) error) error {
	if j.wIdx != nil {
		return ErrNotReadOnly
	}

	var entHeadBuf [8]byte
	entBuf := make([]byte, 1<<20)

	for at := int64(0); at < j.dataLen; {
		if _, err := j.data.ReadAt(entHeadBuf[:], at); err != nil {
			return xerrors.Errorf("reading entry header: %w", err)
		}

		entLen := binary.LittleEndian.Uint32(entHeadBuf[:4]) - 1 - 2
		entType := entHeadBuf[4]
		mhLen := uint32(binary.LittleEndian.Uint16(entHeadBuf[6:]))

		if entType != byte(entBlock) {
			return xerrors.Errorf("unexpected entry type %d, expected block (1)", entType)
		}

		if entLen > uint32(len(entBuf)) {
			// expand buffer to next power of two if needed
			entBuf = make([]byte, 1<<bits.Len32(entLen))
		}

		if _, err := j.data.ReadAt(entBuf[:entLen], at+int64(len(entHeadBuf))); err != nil {
			return xerrors.Errorf("reading entry: %w", err)
		}

		if err := cb(entBuf[entLen-mhLen:entLen], entBuf[:entLen-mhLen]); err != nil {
			return err
		}

		at += int64(len(entHeadBuf)) + int64(entLen)
	}

	return nil
}

/* Finalization */

var ErrReadOnly = errors.New("already read-only")

func (j *JBOB) MarkReadOnly() error {
	if j.wIdx == nil {
		return ErrReadOnly
	}

	err := j.mutHead(func(h *Head) error {
		h.ReadOnly = true
		return nil
	})
	if err != nil {
		return xerrors.Errorf("marking as read-only: %w", err)
	}

	// read index is now the same as the write index, it will get swapped to bsst
	// after finalization
	j.wIdx = nil

	return nil
}

func (j *JBOB) Finalize() error {
	if j.wIdx != nil {
		return xerrors.Errorf("cannot finalize read-write jbob")
	}

	bss, err := CreateBSSTIndex(filepath.Join(j.IndexPath, BsstIndex), j.rIdx)
	if err != nil {
		return xerrors.Errorf("creating bsst index: %w", err)
	}

	err = j.mutHead(func(h *Head) error {
		h.Finalized = true
		return nil
	})
	if err != nil {
		return xerrors.Errorf("marking as finalized: %w", err)
	}

	err = j.rIdx.Close()
	j.rIdx = bss
	if err != nil {
		return err
	}

	return nil
}

func (j *JBOB) DropLevel() error {
	if j.wIdx != nil {
		return xerrors.Errorf("cannot drop level on read-write jbob")
	}

	if err := os.RemoveAll(filepath.Join(j.IndexPath, LevelIndex)); err != nil {
		return xerrors.Errorf("removing leveldb index: %w", err)
	}

	return nil
}

/* MISC */

func (j *JBOB) Close() (int64, error) {
	// sync / close log first
	if err := j.data.Close(); err != nil {
		return 0, xerrors.Errorf("closing data: %w", err)
	}

	// then head
	at, err := j.Commit()
	if err != nil {
		return 0, xerrors.Errorf("committing head: %w", err)
	}

	if err := j.head.Close(); err != nil {
		return 0, xerrors.Errorf("closing head: %w", err)
	}

	// then indexes
	if j.wIdx != nil {
		if err := j.wIdx.Close(); err != nil {
			return 0, xerrors.Errorf("closing index: %w", err)
		}
	} else {
		if err := j.rIdx.Close(); err != nil {
			return 0, xerrors.Errorf("closing index: %w", err)
		}
	}

	return at, nil
}
