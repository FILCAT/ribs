package carlog

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/lotus-web3/ribs/bsst"
	"github.com/minio/sha256-simd"
	mh "github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	LogBufSize = 128 << 10
	LogWriteCh = 128

	LogMaxSize = 1 << 20 // 1 Mi entries

	StringLogExt = ".sl"
	BsstProgExt  = ".bsst.prog"
	BsstExt      = ".bsst"
)

type LogBsstIndex struct {
	root string // root dir

	// mhh salt
	Salt [32]byte

	partitions []*partition // last one is write log, rest is compacting or compacted partitions

	writeLk sync.Mutex
}

type partition struct {
	compacted atomic.Bool // unless compacted read from logIndex

	// Log state

	logFile *os.File

	logIndexLk sync.Mutex // maps ops are fast, so rw lock may actually be slower (todo benchmark)
	logIndex   map[string]int64

	// writing
	writesSent int64
	//writesRecv atomic.Int64
	writesDone atomic.Int64

	writeCh chan []byte
	closing bool

	bw *bufio.Writer

	writeFlushed atomic.Int64

	lastFinishedWrite atomic.Int64

	// Compacted state
}

func OpenLogBsstIndex(root string) (*LogBsstIndex, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, xerrors.Errorf("mkdirall: %w", err)
	}

	lbi := &LogBsstIndex{
		root: root,
	}

	// Load salt (if not present we're creating a new index)

	// Load partitions
	// * Every non-last log is a compacting log
	// * Every log with .bsst is a compacted log
	//   * If a log with .bsst has a .sl file, it's a finished compaction but .sl wasn't removed (we do that as cleanup on startup)
	// * Logs with .bsst.prog are unfinished compactions
	// * Last log is a write log unless it has a .bsst.prog file, in which case it's a compacting log
	// * If there's no last writable log, new one will be created in Put

	return lbi, nil
}

/*
----------------------------
---------- WRITE ----------
----------------------------
*/

func (l *LogBsstIndex) Put(c []mh.Multihash, offs []int64) error {
	if len(c) != len(offs) {
		return xerrors.New("mismatched input lengths")
	}
	if len(c) == 0 {
		return nil
	}

	// make entries
	/*writeBuf := pool.Get(len(c) * bsst.EntrySize)
	defer pool.Put(writeBuf)*/
	writeBuf := make([]byte, len(c)*bsst.EntrySize)

	for i, h := range c {
		k := l.makeMHKey(h, 0) // always 0th instance, we assume entries don't repeat

		// bsst.EntKeyBytes
		copy(writeBuf[i*bsst.EntrySize:], k[:bsst.EntKeyBytes])
		binary.LittleEndian.PutUint64(writeBuf[i*bsst.EntrySize+bsst.EntKeyBytes:], uint64(offs[i]))
	}

	l.writeLk.Lock()

	{
		// ensure space in current log

		needNewLog := true
		if len(l.partitions) > 0 {
			lastLog := l.partitions[len(l.partitions)-1]
			if lastLog.writesSent+int64(len(c)) < LogMaxSize {
				needNewLog = false
			}
		}

		if needNewLog {
			if err := l.newLog(); err != nil {
				l.writeLk.Unlock()
				return err
			}
		}
	}

	l.partitions[len(l.partitions)-1].writesSent += int64(len(c))
	l.partitions[len(l.partitions)-1].writeCh <- writeBuf

	l.partitions[len(l.partitions)-1].logIndexLk.Lock()
	l.writeLk.Unlock()

	for i, h := range c {
		l.partitions[len(l.partitions)-1].logIndex[string(h)] = offs[i]
	}
	l.partitions[len(l.partitions)-1].logIndexLk.Unlock()

	return nil
}

func (l *LogBsstIndex) newLog() error {
	logPath := filepath.Join(l.root, fmt.Sprintf("log%d.sl", len(l.partitions)))

	f, err := os.Create(logPath)
	if err != nil {
		return xerrors.Errorf("create new log file: %w", err)
	}

	ilo := &partition{
		logFile:  f,
		logIndex: map[string]int64{}, // todo map pool?
		writeCh:  make(chan []byte, LogWriteCh),
		bw:       bufio.NewWriterSize(f, LogBufSize),
	}

	prevLast := l.partitions[len(l.partitions)-1]
	close(prevLast.writeCh)

	l.partitions = append(l.partitions, ilo)
	go ilo.run()

	return nil
}

func (l *LogBsstIndex) Del(c []mh.Multihash) error {
	panic("implement me")
}

func (i *partition) run() {
	var writesRecv int64

	for b := range i.writeCh {
		if len(b) == 0 {
			if err := i.flush(writesRecv); err != nil {
				log.Errorf("flushing log: %s", err)
				return
			}
			continue
		}

		writesRecv++

		_, err := i.bw.Write(b)
		if err != nil {
			log.Errorf("writing to log: %s", err)
			return // this will make things hang, but we can't really do anything else (todo wider panik)
		}
	}

	if err := i.flush(writesRecv); err != nil {
		log.Errorf("flushing log: %s", err)
		return
	}

	if i.closing {
		if err := i.logFile.Close(); err != nil {
			log.Errorf("closing log file: %s", err)
			return
		}

		return
	}

	i.compact()
}

func (i *partition) compact() {
	nonSlPath := strings.TrimSuffix(i.logFile.Name(), StringLogExt)
	bsstPath := nonSlPath + BsstExt

	bsst.CreateAdv()
}

func (i *partition) flush(writesRecv int64) error {
	if writesRecv == 0 {
		return nil
	}

	if err := i.bw.Flush(); err != nil {
		return xerrors.Errorf("flushing log: %w", err)
	}

	if err := i.logFile.Sync(); err != nil {
		return xerrors.Errorf("syncing log: %w", err)
	}

	i.writeFlushed.Store(writesRecv)
	return nil
}

/*
----------------------------
----------- READ -----------
----------------------------
*/

func (l *LogBsstIndex) Has(c []mh.Multihash) ([]bool, error) {
	//TODO implement me
	panic("implement me")
}

func (l *LogBsstIndex) Get(c []mh.Multihash) ([]int64, error) {
	//TODO implement me
	panic("implement me")
}

func (l *LogBsstIndex) Entries() (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (l *LogBsstIndex) List(f func(c mh.Multihash, offs []int64) error) error {
	//TODO implement me
	panic("implement me")
}

func (l *LogBsstIndex) ToTruncate(atOrAbove int64) ([]mh.Multihash, error) {
	//TODO implement me
	panic("implement me")
}

func (l *LogBsstIndex) Close() error {
	//TODO implement me
	panic("implement me")
}

type multiHashHash struct {
	mhh [32]byte
	off int64
}

func (l *LogBsstIndex) makeMHH(c mh.Multihash, i int64, off int64) multiHashHash {
	return multiHashHash{
		mhh: l.makeMHKey(c, i),
		off: off,
	}
}

func (l *LogBsstIndex) makeMHKey(c mh.Multihash, i int64) [32]byte {
	// buf = [salt][i: le64][c[:64]]
	var buf [(32 + 8) + (32 * 2)]byte
	copy(buf[:], l.Salt[:])
	binary.LittleEndian.PutUint64(buf[32:], uint64(i))
	copy(buf[32+8:], c)

	return sha256.Sum256(buf[:])
}

var _ WritableIndex = (*LogBsstIndex)(nil)
var _ ReadableIndex = (*LogBsstIndex)(nil)
