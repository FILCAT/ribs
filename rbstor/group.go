package rbstor

import (
	"bufio"
	"context"
	"fmt"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	iface "github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/jbob"
	mh "github.com/multiformats/go-multihash"
	"io"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	"golang.org/x/xerrors"
	"os"
)

var (
	// TODO: make this configurable
	maxGroupSize int64 = 2500 << 20

	// todo enforce this
	maxGroupBlocks int64 = 20 << 20
)

type Group struct {
	db    *rbsDB
	index iface.Index

	//lotusRPCAddr string

	path string
	id   int64

	state iface.GroupState

	// db lock
	// note: can be taken when jblk is held
	dblk sync.Mutex

	// jbob (with jblk)

	jblk sync.RWMutex

	// inflight counters track current jbob writes which are not yet committed
	inflightBlocks int64
	inflightSize   int64

	// committed counters match the db
	committedBlocks int64
	committedSize   int64

	// atomic perf/diag counters
	readBlocks  int64
	readSize    int64
	writeBlocks int64
	writeSize   int64

	// perf counter snapshots, owned by group manager
	readBlocksSnap  int64
	readSizeSnap    int64
	writeBlocksSnap int64
	writeSizeSnap   int64

	jb *jbob.JBOB
}

func OpenGroup(ctx context.Context, db *rbsDB, index iface.Index, id, committedBlocks, committedSize, recordedHead int64, path string, state iface.GroupState, create bool) (*Group, error) {
	groupPath := filepath.Join(path, "grp", strconv.FormatInt(id, 32))

	if err := os.MkdirAll(groupPath, 0755); err != nil {
		return nil, xerrors.Errorf("create group directory: %w", err)
	}

	// open jbob

	jbOpenFunc := jbob.Open
	if create {
		jbOpenFunc = jbob.Create
	}

	jb, err := jbOpenFunc(filepath.Join(groupPath, "blk.jbmeta"), filepath.Join(groupPath, "blk.jblog"), func(to int64, h []mh.Multihash) error {
		if to < recordedHead {
			return xerrors.Errorf("cannot rewind jbob head to %d, recorded group head is %d", to, recordedHead)
		}

		return index.DropGroup(ctx, h, id)
	})
	if err != nil {
		return nil, xerrors.Errorf("open jbob (grp: %s): %w", groupPath, err)
	}

	return &Group{
		db:    db,
		index: index,

		jb: jb,

		committedBlocks: committedBlocks,
		committedSize:   committedSize,

		path:  groupPath,
		id:    id,
		state: state,
	}, nil
}

func (m *Group) Put(ctx context.Context, b []blocks.Block) (int, error) {
	// NOTE: Put is the only method which writes data to jbob

	if len(b) == 0 {
		return 0, nil
	}

	// jbob writes are not thread safe, take the lock to get serial access
	m.jblk.Lock()
	defer m.jblk.Unlock()

	if m.state != iface.GroupStateWritable {
		return 0, nil
	}

	// reserve space
	availSpace := maxGroupSize - m.committedSize - m.inflightSize // todo async - inflight

	var writeSize int64
	var writeBlocks int

	for _, blk := range b {
		if int64(len(blk.RawData()))+writeSize > availSpace {
			break
		}
		writeSize += int64(len(blk.RawData()))
		writeBlocks++
	}

	if writeBlocks < len(b) {
		// this group is full
		m.state = iface.GroupStateFull
	}

	m.inflightBlocks += int64(writeBlocks)
	m.inflightSize += writeSize

	atomic.AddInt64(&m.writeBlocks, int64(writeBlocks))
	atomic.AddInt64(&m.writeSize, writeSize)

	// backend write

	// 1. (buffer) writes to jbob

	c := make([]mh.Multihash, len(b))
	for i, blk := range b {
		c[i] = blk.Cid().Hash()
	}

	err := m.jb.Put(c[:writeBlocks], b[:writeBlocks])
	if err != nil {
		// todo handle properly (abort, close, check disk space / resources, repopen)
		// todo docrement inflight?
		return 0, xerrors.Errorf("writing to jbob: %w", err)
	}

	// 3. write top-level index (before we update group head so replay is possible, before jbob commit so that it's faster)
	//    missed, uncommitted jbob writes should be ignored.
	// ^ TODO: Test this commit edge case
	// TODO: Async index queue
	err = m.index.AddGroup(ctx, c[:writeBlocks], m.id)
	if err != nil {
		// todo handle properly (abort, close, check disk space / resources, repopen)
		return 0, xerrors.Errorf("writing index: %w", err)
	}

	// 3.5 mark as read-only if full
	// todo is this the right place to do this?
	if m.state == iface.GroupStateFull {
		if err := m.sync(ctx); err != nil {
			// todo handle properly (abort, close, check disk space / resources, repopen)
			return 0, xerrors.Errorf("sync full group: %w", err)
		}

		if err := m.jb.MarkReadOnly(); err != nil {
			// todo handle properly (abort, close, check disk space / resources, repopen)
			// todo combine with commit?
			return 0, xerrors.Errorf("mark jbob read-only: %w", err)
		}
	}

	return writeBlocks, nil
}

func (m *Group) Sync(ctx context.Context) error {
	m.jblk.Lock()
	defer m.jblk.Unlock()

	return m.sync(ctx)
}

func (m *Group) sync(ctx context.Context) error {
	fmt.Println("syncing group", m.id)
	// 1. commit jbob (so puts above are now on disk)

	at, err := m.jb.Commit()
	if err != nil {
		// todo handle properly (abort, close, check disk space / resources, repopen)
		return xerrors.Errorf("committing jbob: %w", err)
	}

	// todo with async index queue, also wait for index queue to be flushed

	// 2. update head
	m.committedBlocks += m.inflightBlocks
	m.committedSize += m.inflightSize
	m.inflightBlocks = 0
	m.inflightSize = 0

	m.dblk.Lock()
	err = m.db.SetGroupHead(ctx, m.id, m.state, m.committedBlocks, m.committedSize, at)
	m.dblk.Unlock()
	if err != nil {
		// todo handle properly (retry, abort, close, check disk space / resources, repopen)
		return xerrors.Errorf("update group head: %w", err)
	}

	return nil
}

func (m *Group) Unlink(ctx context.Context, c []mh.Multihash) error {
	// write log

	// write idx

	// update head

	//TODO implement me
	panic("implement me")
}

func (m *Group) View(ctx context.Context, c []mh.Multihash, cb func(cidx int, data []byte)) error {
	// right now we just read from jbob

	// todo: Technically we only need this lock when jbob is writable
	m.jblk.Lock()
	defer m.jblk.Unlock()

	return m.jb.View(c, func(cidx int, found bool, data []byte) error {
		// TODO: handle not found better?
		if !found {
			return xerrors.Errorf("group: block not found")
		}

		m.jblk.Unlock()
		defer m.jblk.Lock()

		atomic.AddInt64(&m.readBlocks, 1)
		atomic.AddInt64(&m.readSize, int64(len(data)))

		cb(cidx, data)
		return nil
	})
}

func (m *Group) Close() error {
	if err := m.Sync(context.Background()); err != nil {
		return err
	}

	m.jblk.Lock()
	defer m.jblk.Unlock()

	_, err := m.jb.Close()
	// todo mark as closed
	return err
}

type sizerWriter struct {
	w io.Writer
	s int64
}

func (s *sizerWriter) Write(p []byte) (int, error) {
	w, err := s.w.Write(p)
	s.s += int64(w)
	return w, err
}

// returns car size and root cid
func (m *Group) writeCar(w io.Writer) (int64, cid.Cid, error) {
	// read layers file
	ls, err := os.ReadFile(filepath.Join(m.path, "vcar", "layers"))
	if err != nil {
		return 0, cid.Undef, xerrors.Errorf("read layers file: %w", err)
	}

	layerCount, err := strconv.Atoi(string(ls))
	if err != nil {
		return 0, cid.Undef, xerrors.Errorf("parse layers file: %w", err)
	}

	// read arity file
	as, err := os.ReadFile(filepath.Join(m.path, "vcar", "arity"))
	if err != nil {
		return 0, cid.Undef, xerrors.Errorf("read arity file: %w", err)
	}

	arity, err := strconv.Atoi(string(as))
	if err != nil {
		return 0, cid.Undef, xerrors.Errorf("parse arity file: %w", err)
	}

	var layers []*cardata

	for i := 1; i <= layerCount; i++ {
		fname := filepath.Join(m.path, "vcar", fmt.Sprintf("layer%d.cardata", i))
		f, err := os.OpenFile(fname, os.O_RDONLY, 0644)
		if err != nil {
			return 0, cid.Undef, xerrors.Errorf("open cardata file: %w", err)
		}

		layers = append(layers, &cardata{
			f:  f,
			br: bufio.NewReader(f),
		})
	}

	defer func() {
		for n, l := range layers {
			if err := l.f.Close(); err != nil {
				log.Warnf("closing cardata layer %d file: %s", n+1, err)
			}
		}
	}()

	// read root block, which is the only block in the last layer
	rcid, _, err := carutil.ReadNode(layers[len(layers)-1].br)
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
	_, err = layers[len(layers)-1].f.Seek(0, io.SeekStart)
	if err != nil {
		return 0, cid.Undef, xerrors.Errorf("seeking to start of last layer: %w", err)
	}
	layers[len(layers)-1].br.Reset(layers[len(layers)-1].f)

	// write depth first, starting from top layer
	atLayer := layerCount

	layerWrote := make([]int, layerCount+1)

	err = m.jb.Iterate(func(c mh.Multihash, data []byte) error {
		// get down to layer 0 (jbob)
		for atLayer > 0 {
			// read next block from current layer
			c, data, err := carutil.ReadNode(layers[atLayer-1].br)
			if err != nil {
				return xerrors.Errorf("reading node from layer %d (wrote %d at that layer): %w", atLayer, layerWrote[atLayer], err)
			}

			// write block
			if err := carutil.LdWrite(w, c.Bytes(), data); err != nil {
				return xerrors.Errorf("writing node from layer %d: %w", atLayer, err)
			}

			layerWrote[atLayer]++
			atLayer--
		}

		// write block
		if err := carutil.LdWrite(w, mhToRawCid(c).Bytes(), data); err != nil {
			return xerrors.Errorf("writing jbob block: %w", err)
		}

		layerWrote[atLayer]++

		// propagate layers up if at arity
		for layerWrote[atLayer] == arity {
			layerWrote[atLayer] = 0
			atLayer++
		}

		return nil
	})
	if err != nil {
		return 0, cid.Undef, xerrors.Errorf("iterate jbob: %w", err)
	}

	return sw.s, rcid, nil
}

type cardata struct {
	f *os.File

	br *bufio.Reader
}

func (c *cardata) writeBlock(ci cid.Cid, data []byte) error {
	return carutil.LdWrite(c.f, ci.Bytes(), data)
}

func mhToRawCid(mh mh.Multihash) cid.Cid {
	return cid.NewCidV1(cid.Raw, mh)
}

var _ iface.Group = &Group{}
