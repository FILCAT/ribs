package carlog

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/filecoin-project/lotus/lib/must"
	lru "github.com/hashicorp/golang-lru/v2"
	cbor "github.com/ipfs/go-ipld-cbor"
	carutil "github.com/ipld/go-car/util"
	"golang.org/x/xerrors"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"github.com/ipld/go-car"
	pool "github.com/libp2p/go-buffer-pool"

	"github.com/lotus-web3/ribs/bsst"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("carlog")

const (
	HeadName = "head"
	HeadSize = 512

	LevelIndex = "index.level"
	BsstIndex  = "index.bsst"
	HashSample = "sample.mhlist"
)

const jbobBufSize = 16 << 20

// entries is not the perfect metric, but, roughly we want jbobBufSize at 256k blocks
// less would actually be fine too, but would make that buffer very large with big blocks
const writeLRUEntries = 128

// placeholderCid is a placeholder CID which has the same length as dag-cbor-sha256 CID
var placeholderCid = func() cid.Cid {
	data := make([]byte, 32)
	multihash, err := mh.Sum(data, mh.IDENTITY, len(data))
	if err != nil {
		panic("Error creating multihash: " + err.Error())
	}
	return cid.NewCidV1(cid.Raw, multihash)
}()

// CarLog is a .car file which is storing a flat layer of blocks, with a wide
// top tree and a single root
// * Before the CarLog is finalized, the root CID is a placeholher Identity hash
// * Blocks are stored layer-by-layer, from the bottom, left to right
// * Transforming into depth-first .car is very cheap with the head file
// * NOT THREAD SAFE FOR WRITING!!
//   - Reads can happen in parallel with writing
//
// * One tx at a time
// * Not considered written until committed
type CarLog struct {
	// index = dir, data = file
	IndexPath, DataPath string

	// head is a file which contains cbor-map-serialized Head, padded up to head
	// size
	head *os.File

	// data contains a log of all written data
	// [carv1 header][carv1 block...]
	data *os.File

	// dataPos wraps data file, and keeps track of the current position
	dataPos *appendCounter

	// dataBuffered wraps dataPos, and buffers writes
	dataBuffered *bufio.Writer // todo free in finalize
	dataBufLk    sync.Mutex

	// current data file length
	dataLen int64

	dataStart    int64 // length of the carv1 header
	dataEnd      int64 // byte offset of the end of the last layer
	layerOffsets []int64

	// index

	idxLk sync.RWMutex
	wIdx  WritableIndex
	rIdx  ReadableIndex

	// buffers
	headBuf [HeadSize]byte

	// todo benchmark this LRU, make sure it actually makes things faster
	// todo drop in Finalize?
	writeLru *lru.Cache[int64, []byte] // offset -> data

	// readStateLk is used for checking / accessing state which needs to be
	// checked before doing reads (layerOffsets.len)
	readStateLk sync.Mutex

	// pendingReads is used to wait for pending reads to finish before offloading data
	// (protects the data file)
	pendingReads sync.WaitGroup
}

// Head is the on-disk head object. CBOR-map-serialized. Must fit in
//
//	HeadSize bytes. Null-Padded to exactly HeadSize
type Head struct {
	Version int64

	// something that's not zero
	Valid bool

	// byte offset just after the last retired op. If finalized, but layers aren't set
	// this points to the end of first (bottom) layer
	RetiredAt int64

	DataStart int64

	// byte offset of the start of the second layer
	DataEnd int64

	ReadOnly  bool // if true, no more writes are allowed
	Finalized bool // if true, no more writes are allowed, and the bsst index is finalized
	Offloaded bool // if true, the data file is offloaded to external storage, only hash samples are kept

	// Layer stats, set after Finalized (Finalized can be true and layers may still not be set)
	LayerOffsets []int64 // byte offsets of the start of each layer
}

func Create(indexPath, dataPath string, _ TruncCleanup) (*CarLog, error) {
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

	// setup carv1 header
	carHead := &car.CarHeader{
		Roots:   []cid.Cid{placeholderCid},
		Version: 1,
	}
	if err := car.WriteHeader(carHead, dataFile); err != nil {
		return nil, xerrors.Errorf("writing placeholder header: %w", err)
	}
	if err := dataFile.Sync(); err != nil {
		return nil, xerrors.Errorf("sync new data file: %w", err)
	}

	at, err := car.HeaderSize(carHead)
	if err != nil {
		return nil, xerrors.Errorf("getting car header length: %w", err)
	}

	// write head file
	h := &Head{
		Valid:     true,
		RetiredAt: int64(at),
		DataStart: int64(at),
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

	ac := &appendCounter{dataFile, int64(at)}

	return &CarLog{
		IndexPath:    indexPath,
		DataPath:     dataPath,
		head:         headFile,
		data:         dataFile,
		dataPos:      ac,
		dataBuffered: bufio.NewWriterSize(ac, jbobBufSize),
		dataLen:      int64(at),
		dataStart:    int64(at),

		wIdx: idx,
		rIdx: idx,

		writeLru: must.One(lru.New[int64, []byte](writeLRUEntries)),
	}, nil
}

func Open(indexPath, dataPath string, tc TruncCleanup) (*CarLog, error) {
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

	if h.Offloaded {
		return &CarLog{
			IndexPath: indexPath,
			DataPath:  dataPath,

			layerOffsets: h.LayerOffsets,
		}, nil
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

	if dataInfo.Size() < h.RetiredAt {
		// something is not yes

		return nil, xerrors.Errorf("data file is shorter than head says it should be (%d < %d)", dataInfo.Size(), h.RetiredAt)
	}

	jb := &CarLog{
		IndexPath: indexPath,
		DataPath:  dataPath,
		head:      headFile,
		data:      dataFile,
		dataLen:   dataInfo.Size(),

		dataStart:    h.DataStart,
		dataEnd:      h.DataEnd,
		layerOffsets: h.LayerOffsets,

		writeLru: must.One(lru.New[int64, []byte](writeLRUEntries)),
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

	if dataInfo.Size() > h.RetiredAt { // data ahead means there was an unclean shutdown during a write
		if h.ReadOnly {
			// If the file is truncated while being marked as read-only, something is terribly wrong.
			return nil, xerrors.Errorf("read only(!) data file is shorter than head says it should be (%d < %d)", dataInfo.Size(), h.RetiredAt)
		}

		//return nil, xerrors.Errorf("data file is shorter than head says it should be (%d < %d)", dataInfo.Size(), h.RetiredAt)

		// truncate index
		err := jb.truncate(h.RetiredAt, dataInfo.Size(), tc)
		if err != nil {
			return nil, xerrors.Errorf("truncating jbob: %w", err)
		}
	}

	// seek to data end as writes are appended
	if _, err := dataFile.Seek(jb.dataLen, io.SeekStart); err != nil {
		return nil, xerrors.Errorf("seeking to data end: %w", err)
	}

	jb.dataPos = &appendCounter{dataFile, jb.dataLen}
	jb.dataBuffered = bufio.NewWriterSize(jb.dataPos, jbobBufSize)

	return jb, nil
}

type TruncCleanup func(to int64, h []mh.Multihash) error

func (j *CarLog) truncate(offset, size int64, onRemove TruncCleanup) error {
	if j.wIdx == nil {
		return xerrors.Errorf("cannot truncate a read-only jbob")
	}

	if onRemove == nil {
		return xerrors.Errorf("onRemove callback is nil")
	}

	toTruncate, err := j.wIdx.ToTruncate(offset)
	if err != nil {
		return xerrors.Errorf("getting multihashes to truncate: %w", err)
	}

	log.Errorw("truncate", "offset", offset, "size", size, "diff", size-offset, "idxEnts", len(toTruncate), "dataPath", j.DataPath)

	if len(toTruncate) > 0 {
		if err := onRemove(offset, toTruncate); err != nil {
			return xerrors.Errorf("truncate callback error: %w", err)
		}
		if err := j.wIdx.Del(toTruncate); err != nil {
			return xerrors.Errorf("deleting multihashes from jbob index: %w", err)
		}
	}

	if err := j.data.Truncate(offset); err != nil {
		return xerrors.Errorf("truncating data file: %w", err)
	}

	j.dataLen = offset

	// Update the head
	err = j.mutHead(func(h *Head) error {
		h.RetiredAt = offset
		return nil
	})

	if err != nil {
		return xerrors.Errorf("updating head after truncate: %w", err)
	}

	return nil
}

/* WRITE SIDE */

type WritableIndex interface {
	// Put records entries in the index
	// sync for now, todo
	// -1 offset means 'skip'
	Put(c []mh.Multihash, offs []int64) error

	Del(c []mh.Multihash) error

	// Truncate returns a list of multihashes to remove from the index
	ToTruncate(atOrAbove int64) ([]mh.Multihash, error)

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

func (j *CarLog) mutHead(mut func(h *Head) error) error {
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

func (j *CarLog) Put(c []mh.Multihash, b []blocks.Block) error {
	j.idxLk.RLock()
	defer j.idxLk.RUnlock()

	if j.wIdx == nil {
		return xerrors.Errorf("cannot write to read-only (or closing) jbob")
	}

	if len(c) != len(b) {
		return xerrors.Errorf("hash list length doesn't match blocks length")
	}
	offsets := make([]int64, len(b))

	hasList, err := j.rIdx.Has(c)
	if err != nil {
		return err
	}

	// first append to log
	for i, blk := range b {
		if hasList[i] {
			offsets[i] = -1
			continue
		}

		if MaxEntryLen < len(blk.RawData()) {
			return xerrors.Errorf("block too large (%d bytes, max %d)", len(blk.RawData()), MaxEntryLen)
		}

		// todo use a buffer with fixed cid prefix to avoid allocs
		bcid := cid.NewCidV1(cid.Raw, c[i]).Bytes()

		offsets[i] = makeOffsetLen(j.dataLen, len(bcid)+len(blk.RawData()))

		n, err := j.ldWrite(bcid, blk.RawData())
		if err != nil {
			return xerrors.Errorf("writing block: %w", err)
		}

		j.writeLru.Add(offsets[i], blk.RawData())

		j.dataLen += n
	}

	// log the write
	// todo async

	if err := j.wIdx.Put(c, offsets); err != nil {
		return xerrors.Errorf("updating index: %w", err)
	}

	return nil
}

func (j *CarLog) ldWrite(d ...[]byte) (int64, error) {
	var sum uint64
	for _, s := range d {
		sum += uint64(len(s))
	}

	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, sum)

	j.dataBufLk.Lock()
	defer j.dataBufLk.Unlock()

	_, err := j.dataBuffered.Write(buf[:n])
	if err != nil {
		// todo flag as corrupt
		return 0, err
	}

	for _, s := range d {
		_, err = j.dataBuffered.Write(s)
		if err != nil {
			// todo flag as corrupt
			return 0, err
		}
	}

	return int64(sum) + int64(n), nil
}

var errNothingToCommit = errors.New("nothing to commit")

func (j *CarLog) Commit() (int64, error) {
	// todo log commit?

	if err := j.flushBuffered(); err != nil {
		return 0, xerrors.Errorf("flushing buffered data: %w", err)
	}

	// todo call this on directory fd?
	if err := j.data.Sync(); err != nil {
		return 0, xerrors.Errorf("sync data: %w", err)
	}

	// todo index is sync for now, and we're single threaded, so if there were any
	// puts, just update head

	err := j.mutHead(func(h *Head) error {
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

func (j *CarLog) flushBuffered() error {
	j.dataBufLk.Lock()
	defer j.dataBufLk.Unlock()

	if j.dataBuffered.Buffered() > 0 {
		for {
			err := j.dataBuffered.Flush()
			if err != io.ErrShortWrite {
				return err
			}
		}
	}

	return nil
}

/* READ SIDE */

func (j *CarLog) View(c []mh.Multihash, cb func(cidx int, found bool, data []byte) error) error {
	j.idxLk.RLock()
	if j.rIdx == nil {
		j.idxLk.RUnlock()
		return xerrors.Errorf("cannot read from closing or offloaded carlog")
	}
	locs, err := j.rIdx.Get(c)

	j.pendingReads.Add(1)
	j.idxLk.RUnlock()
	defer j.pendingReads.Done()
	if err != nil {
		return xerrors.Errorf("getting value locations: %w", err)
	}

	entBuf := pool.Get(1 << 20)

	dataAt := j.dataPos.Pos()

	for i := range c {
		if locs[i] == -1 {
			if err := cb(i, false, nil); err != nil {
				return err
			}
			continue
		}

		if v, ok := j.writeLru.Get(locs[i]); ok {
			if err := cb(i, true, v); err != nil {
				return err
			}
			continue
		}

		off, entLen := fromOffsetLen(locs[i])

		if entLen > len(entBuf) {
			// expand buffer to next power of two if needed
			pool.Put(entBuf)
			entBuf = pool.Get(1 << bits.Len32(uint32(entLen)))
		}

		// calculate length of length prefix
		// note this uses entBuf as a buffer, but we don't need the content
		lenlen := binary.PutUvarint(entBuf, uint64(entLen))

		// check if dataAt (size of written data) is larger than the offset+size of the entry
		if dataAt < off+int64(lenlen)+int64(entLen) {
			// probably need to flush the buffer

			// first, re-check if we actually need to do that
			dataAt = j.dataPos.Pos()
			if dataAt < off+int64(lenlen)+int64(entLen) {
				// yeah..
				if err := j.flushBuffered(); err != nil {
					return xerrors.Errorf("flushing buffered data: %w", err)
				}

				//log.Errorw("flush in read path", "toFlush", dataAt-(off+int64(lenlen)+int64(entLen)), "dataAt", dataAt)

				dataAt = j.dataPos.Pos()
				if dataAt < off+int64(lenlen)+int64(entLen) {
					diff := off + int64(lenlen) + int64(entLen) - dataAt
					return xerrors.Errorf("entry beyond range after flush, dataAt (%d) < off (%d) + lenlen (%d) + entLen (%d) (diff: %d)", dataAt, off, lenlen, entLen, diff)
				}
			}
		}

		// READ!
		if _, err := j.data.ReadAt(entBuf[:entLen], off+int64(lenlen)); err != nil {
			return xerrors.Errorf("reading entry: %w", err)
		}

		n, _, err := cid.CidFromBytes(entBuf[:entLen])
		if err != nil {
			return xerrors.Errorf("parsing cid: %w", err)
		}

		// NOTE: THIS callback MAY UNLOCK THE LOG LOCK
		if err := cb(i, true, entBuf[n:entLen]); err != nil {
			return err
		}
	}

	pool.Put(entBuf)

	return nil
}

var ErrNotReadOnly = errors.New("not yet read-only")

func (j *CarLog) iterate(cb func(c cid.Cid, data []byte) error) error {
	if j.wIdx != nil {
		return ErrNotReadOnly
	}

	if j.dataEnd == 0 {
		return xerrors.New("can't iterate yet - data end not marked")
	}

	entBuf := make([]byte, 1<<20)

	rs := &readSeekerFromReaderAt{
		readerAt: j.data,
		pos:      j.dataStart,
	}

	br := bufio.NewReaderSize(io.LimitReader(rs, j.dataEnd-j.dataStart), 4<<20)

	for {
		if _, err := br.Peek(1); err != nil { // no more blocks, likely clean io.EOF
			if err == io.EOF {
				return nil
			}
			return err
		}

		entLen, err := binary.ReadUvarint(br)
		if err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF // don't silently pretend this is a clean EOF
			}
			return err
		}

		if entLen > uint64(carutil.MaxAllowedSectionSize) { // Don't OOM
			return errors.New("malformed car; header is bigger than util.MaxAllowedSectionSize")
		}
		if entLen > uint64(len(entBuf)) {
			// expand buffer to next power of two if needed
			entBuf = make([]byte, 1<<bits.Len32(uint32(entLen)))
		}

		if _, err := io.ReadFull(br, entBuf[:entLen]); err != nil {
			return xerrors.Errorf("reading entry: %w", err)
		}

		n, c, err := cid.CidFromBytes(entBuf[:entLen])
		if err != nil {
			return xerrors.Errorf("parsing cid: %w", err)
		}

		if err := cb(c, entBuf[n:entLen]); err != nil {
			return err
		}
	}
}

/* Finalization (marking bottom layer read only, generating fast index) */

var ErrReadOnly = errors.New("already read-only")

func (j *CarLog) MarkReadOnly() error {
	j.idxLk.Lock()
	defer j.idxLk.Unlock()

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

func (j *CarLog) Finalize() error {
	j.idxLk.Lock()
	defer j.idxLk.Unlock()

	if j.wIdx != nil {
		return xerrors.Errorf("cannot finalize read-write jbob")
	}

	var fin, hasTop bool
	err := j.mutHead(func(h *Head) error {
		fin = h.Finalized
		hasTop = len(h.LayerOffsets) > 0
		return nil
	})
	if err != nil {
		return xerrors.Errorf("checking if finalized: %w", err)
	}

	if !fin {
		bss, err := CreateBSSTIndex(filepath.Join(j.IndexPath, BsstIndex), j.rIdx)
		if err != nil {
			return xerrors.Errorf("creating bsst index: %w", err)
		}

		if err := SaveMHList(filepath.Join(j.IndexPath, HashSample), bss.bsi.CreateSample); err != nil {
			return xerrors.Errorf("saving hash sample: %w", err)
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
	}

	if !hasTop {
		if err := j.genTopCar(); err != nil {
			return xerrors.Errorf("generating top car: %w", err)
		}
	}

	return nil
}

func (j *CarLog) DropLevel() error {
	if j.wIdx != nil {
		return xerrors.Errorf("cannot drop level on read-write jbob")
	}

	if err := os.RemoveAll(filepath.Join(j.IndexPath, LevelIndex)); err != nil {
		return xerrors.Errorf("removing leveldb index: %w", err)
	}

	return nil
}

// ARITY IS A FUNDAMENTAL PARAMETER, IT CANNOT BE CHANGED without rewriting all data
const arity = 2048

/* TOP TREE GENERATION */

func (j *CarLog) genTopCar() error {
	if j.wIdx != nil {
		return xerrors.Errorf("cannot generate top car on writable carlog")
	}
	if err := j.flushBuffered(); err != nil {
		return xerrors.Errorf("flushing buffered data: %w", err)
	}
	if err := j.data.Sync(); err != nil {
		return xerrors.Errorf("sync data: %w", err)
	}

	if j.dataEnd != 0 {
		return xerrors.Errorf("cannot generate top car - already generated")
	}
	j.dataEnd = j.dataLen

	var layerOffsets = []int64{0, j.dataLen}

	var curLinks, nextLinks []cid.Cid

	writeLinkBlock := func(links []cid.Cid) error {
		nd, err := cbor.WrapObject(links, mh.SHA2_256, -1)
		if err != nil {
			return xerrors.Errorf("wrap links: %w", err)
		}

		nextLinks = append(nextLinks, nd.Cid())

		n, err := j.ldWrite(nd.Cid().Bytes(), nd.RawData())
		if err != nil {
			return xerrors.Errorf("writing block: %w", err)
		}
		j.dataLen += n

		return nil
	}

	err := j.iterate(func(c cid.Cid, data []byte) error {
		curLinks = append(curLinks, c)

		if len(curLinks) == arity {
			if err := writeLinkBlock(curLinks); err != nil {
				return xerrors.Errorf("writing link block: %w", err)
			}
			curLinks = curLinks[:0]
		}

		return nil
	})
	if err != nil {
		return xerrors.Errorf("iterate jbob: %w", err)
	}

	if len(curLinks) > 0 {
		if err := writeLinkBlock(curLinks); err != nil {
			return xerrors.Errorf("writing link block: %w", err)
		}
		curLinks = curLinks[:0]
	}

	for len(nextLinks) > 1 {
		curLinks, nextLinks = nextLinks, curLinks[:0]

		layerOffsets = append(layerOffsets, j.dataLen)

		for i := 0; i < len(curLinks); i += arity {
			end := i + arity
			if end > len(curLinks) {
				end = len(curLinks)
			}

			if err := writeLinkBlock(curLinks[i:end]); err != nil {
				return xerrors.Errorf("writing link block: %w", err)
			}
		}
	}

	if len(nextLinks) != 1 {
		return xerrors.Errorf("expected 1 link to top layer, got %d", len(nextLinks))
	}

	if err := j.flushBuffered(); err != nil {
		return xerrors.Errorf("flushing buffered data: %w", err)
	}
	if err := j.data.Sync(); err != nil {
		return xerrors.Errorf("sync data: %w", err)
	}

	// read current car header, and update it
	{
		var headerBuf [128]byte
		_, err = j.data.ReadAt(headerBuf[:], 0)
		if err != nil {
			return xerrors.Errorf("reading data car header: %w", err)
		}

		hlen, n := binary.Uvarint(headerBuf[:])
		if n <= 0 {
			return xerrors.Errorf("reading data car header invalid length: %d", n)
		}

		layerOffsets[0] = int64(n) + int64(hlen)

		var header car.CarHeader
		if err := cbor.DecodeInto(headerBuf[n:n+int(hlen)], &header); err != nil {
			return xerrors.Errorf("invalid header: %v", err)
		}
		if header.Version != 1 {
			return xerrors.Errorf("invalid header version: %d", header.Version)
		}
		if len(header.Roots) != 1 {
			return xerrors.Errorf("expected 1 root, got %d", len(header.Roots))
		}
		header.Roots[0] = nextLinks[0]

		headerBytes, err := cbor.DumpObject(header)
		if err != nil {
			return xerrors.Errorf("dumping header: %w", err)
		}
		if uint64(len(headerBytes)) != hlen {
			return xerrors.Errorf("invalid header length, expected %d, got %d", hlen, len(headerBytes))
		}

		copy(headerBuf[n:], headerBytes)

		if _, err := j.data.WriteAt(headerBuf[:n+int(hlen)], 0); err != nil {
			return xerrors.Errorf("writing updated header: %w", err)
		}

		if err := j.data.Sync(); err != nil {
			return xerrors.Errorf("sync data with final header: %w", err)
		}
	}

	j.readStateLk.Lock()
	j.layerOffsets = layerOffsets

	err = j.mutHead(func(h *Head) error {
		h.LayerOffsets = layerOffsets
		h.DataEnd = layerOffsets[1]
		h.RetiredAt = j.dataLen

		return nil
	})

	j.readStateLk.Unlock()

	return err
}

/* CANONICAL CAR OUTPUT */

// returns car size and root cid
func (j *CarLog) WriteCar(w io.Writer) (int64, cid.Cid, error) {
	j.readStateLk.Lock()
	if len(j.layerOffsets) == 0 {
		j.readStateLk.Unlock()
		return 0, cid.Undef, xerrors.Errorf("no layers, finalize first")
	}

	// TODO: Make sure this works on Offloaded groups as expected!!

	j.readStateLk.Unlock()

	var layers []*cardata

	for _, offset := range j.layerOffsets {
		rs := &readSeekerFromReaderAt{
			readerAt: j.data,
			base:     offset,
		}

		layers = append(layers, &cardata{
			rs: rs,
			br: bufio.NewReaderSize(rs, 4<<20),
		})
	}
	if len(layers) == 0 {
		// this can't happen, but without this check lint complains
		return 0, cid.Undef, xerrors.Errorf("somehow ended up with no layers")
	}

	// read root block, which is the only block in the last layer
	rcid, node, err := carutil.ReadNode(layers[len(layers)-1].br)
	if err != nil {
		return 0, cid.Undef, xerrors.Errorf("reading root block: %w", err)
	}

	// todo consider buffering the writes

	sw := &sizerWriter{w: w}
	w = sw

	if err := car.WriteHeader(&car.CarHeader{
		Roots:   []cid.Cid{rcid},
		Version: 1,
	}, w); err != nil {
		return 0, cid.Undef, xerrors.Errorf("write car header: %w", err)
	}
	_, err = layers[len(layers)-1].rs.Seek(0, io.SeekStart)
	if err != nil {
		return 0, cid.Undef, xerrors.Errorf("seeking to start of last layer: %w", err)
	}
	layers[len(layers)-1].br.Reset(layers[len(layers)-1].rs)

	// write depth first, starting from top layer
	atLayer := len(layers) - 1
	var writeNode func(c cid.Cid, data []byte, atLayer int) error
	writeNode = func(c cid.Cid, data []byte, atLayer int) error {
		// write block
		if err := carutil.LdWrite(w, c.Bytes(), data); err != nil {
			return xerrors.Errorf("writing node from layer %d: %w", atLayer, err)
		}

		// if it's a leaf, we're done
		if c.Type() == cid.Raw {
			return nil
		}

		// otherwise, read the blocks from the next layer down recursively

		// TODO: (optimization) Instead of decoding all blocks, just assume that
		//  there are up to /arity/ blocks in each node, and just output the next
		//  layer ountil layerOffsets tell us we're at the end of the layer
		var links []cid.Cid
		if err := cbor.DecodeInto(data, &links); err != nil {
			return xerrors.Errorf("decoding layer links: %w", err)
		}

		for _, ci := range links {
			// TODO: Optimization: instead of reading the whole node, just read
			//  the varint at the start, and copy the whole node (avoids CID decoding)
			c, data, err := carutil.ReadNode(layers[atLayer-1].br)
			if err != nil {
				return xerrors.Errorf("reading node from layer %d: %w", atLayer, err)
			}
			if c != ci {
				return xerrors.Errorf("expected cid %s, got %s, layer %d", ci, c, atLayer)
			}

			// write block
			if err := writeNode(c, data, atLayer-1); err != nil {
				return err
			}
		}

		return nil
	}

	if err := writeNode(rcid, node, atLayer); err != nil {
		return 0, cid.Undef, xerrors.Errorf("writing canonical tree: %w", err)
	}

	return sw.s, rcid, nil
}

type cardata struct {
	rs io.ReadSeeker
	br *bufio.Reader
}

func (j *CarLog) HashSample() ([]mh.Multihash, error) {
	j.readStateLk.Lock()
	if len(j.layerOffsets) == 0 {
		return nil, xerrors.Errorf("cannot read hash sample in a non-finalized car log")
	}
	j.readStateLk.Unlock()

	out, err := LoadMHList(filepath.Join(j.IndexPath, HashSample))
	if err != nil {
		return nil, xerrors.Errorf("loading hash sample: %w", err)
	}

	return out, nil
}

func (j *CarLog) Offload() error {
	j.readStateLk.Lock()

	// first assert that we're finalized, and it's safe to offload
	if len(j.layerOffsets) == 0 {
		j.readStateLk.Unlock()
		return xerrors.Errorf("cannot offload in a non-finalized car log")
	}
	defer j.readStateLk.Unlock()

	j.idxLk.Lock()
	if j.wIdx != nil {
		j.idxLk.Unlock()
		// this can't really happen
		return xerrors.Errorf("group still writable")
	}

	if j.rIdx == nil {
		j.idxLk.Unlock()
		return xerrors.Errorf("group already offloaded")
	}

	// mark as offloaded first
	err := j.mutHead(func(h *Head) error {
		h.Offloaded = true
		return nil
	})
	if err != nil {
		j.idxLk.Unlock()
		return xerrors.Errorf("updating head to mark as offloaded: %w", err)
	}

	if err := j.rIdx.Close(); err != nil {
		j.idxLk.Unlock()
		return xerrors.Errorf("closing readable index: %w", err)
	}

	j.rIdx = nil

	// let reads resume (and fail)
	j.idxLk.Unlock()

	// wait for all readers to finish
	j.pendingReads.Wait()

	// close the data file
	if err := j.data.Close(); err != nil {
		return xerrors.Errorf("closing data file: %w", err)
	}

	// close the head
	if err := j.head.Close(); err != nil {
		return xerrors.Errorf("closing head: %w", err)
	}

	// mark some fields as nil to allow GC
	j.data = nil
	j.dataPos = nil
	j.dataBuffered = nil
	j.writeLru = nil

	// remove index
	if err := os.RemoveAll(filepath.Join(j.IndexPath, BsstIndex)); err != nil {
		return xerrors.Errorf("removing bsst index: %w", err)
	}

	// remove data file
	if err := os.RemoveAll(j.DataPath); err != nil {
		return xerrors.Errorf("removing data file: %w", err)
	}

	return nil
}

func (j *CarLog) Close() error {
	j.idxLk.Lock()
	ri := j.rIdx
	wi := j.wIdx
	j.rIdx = nil
	j.wIdx = nil
	j.idxLk.Unlock()

	j.pendingReads.Wait() // writes hold idxLk

	if ri == nil {
		// either already closed, or offloaded
		return nil
	}

	// then log
	_, err := j.Commit()
	if err != nil {
		return xerrors.Errorf("committing head: %w", err)
	}

	// sync / close head first
	if err := j.data.Close(); err != nil {
		return xerrors.Errorf("closing data: %w", err)
	}

	if err := j.head.Close(); err != nil {
		return xerrors.Errorf("closing head: %w", err)
	}

	// then indexes
	if wi != nil {
		if err := wi.Close(); err != nil {
			return xerrors.Errorf("closing index: %w", err)
		}
	} else {
		if err := ri.Close(); err != nil {
			return xerrors.Errorf("closing index: %w", err)
		}
	}

	return nil
}

/* MISC */

type sizerWriter struct {
	w io.Writer
	s int64
}

func (s *sizerWriter) Write(p []byte) (int, error) {
	w, err := s.w.Write(p)
	s.s += int64(w)
	return w, err
}

// probably a milionth time this helper was created
type readSeekerFromReaderAt struct {
	readerAt io.ReaderAt
	base     int64
	pos      int64
}

func (rs *readSeekerFromReaderAt) Read(p []byte) (n int, err error) {
	n, err = rs.readerAt.ReadAt(p, rs.pos+rs.base)
	rs.pos += int64(n)
	return n, err
}

func (rs *readSeekerFromReaderAt) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		rs.pos = offset
	case io.SeekCurrent:
		rs.pos += offset
	case io.SeekEnd:
		return 0, io.ErrUnexpectedEOF
	default:
		return 0, os.ErrInvalid
	}

	return rs.pos, nil
}

const MaxEntryLen = 1 << (64 - 40)

func makeOffsetLen(off int64, length int) int64 {
	return (int64(length) << 40) | (off & 0xFFFF_FFFF_FF)
}

func fromOffsetLen(offlen int64) (int64, int) {
	return offlen & 0xFFFF_FFFF_FF, int(offlen >> 40)
}

type appendCounter struct {
	w   io.Writer
	pos int64
}

func (ac *appendCounter) Write(p []byte) (int, error) {
	n, err := ac.w.Write(p)
	atomic.AddInt64(&ac.pos, int64(n))
	return n, err
}

func (ac *appendCounter) Pos() int64 {
	return atomic.LoadInt64(&ac.pos)
}
