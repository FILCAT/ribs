package rbstor

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	iface "github.com/atboosty/ribs"
	"github.com/atboosty/ribs/carlog"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"golang.org/x/sync/errgroup"

	"os"

	"golang.org/x/xerrors"
)

var (
	// TODO: make this configurable
	maxGroupSize int64 = 30500 << 20

	// todo enforce this
	maxGroupBlocks int64 = 20 << 20
)

var ErrOffloaded = fmt.Errorf("group is offloaded")

type Group struct {
	db    *rbsDB
	index iface.Index

	staging *atomic.Pointer[iface.StagingStorageProvider]

	path string
	id   int64

	// access with dataLk
	state iface.GroupState

	// db lock
	// note: can be taken when dataLk is held
	dblk sync.Mutex

	// data lock
	dataLk sync.RWMutex

	// reader protectors
	readers   sync.WaitGroup
	offloaded atomic.Int64

	// inflight counters track current jbob writes which are not yet committed
	inflightBlocks int64
	inflightSize   int64

	// committed counters match the db
	committedBlocks int64
	committedSize   int64

	// atomic perf/diag counters
	readBlocks  atomic.Int64
	readSize    atomic.Int64
	writeBlocks atomic.Int64
	writeSize   atomic.Int64

	// perf counter snapshots, owned by group manager
	readBlocksSnap  int64
	readSizeSnap    int64
	writeBlocksSnap int64
	writeSizeSnap   int64

	jb *carlog.CarLog
}

func OpenGroup(ctx context.Context, db *rbsDB, index iface.Index, staging *atomic.Pointer[iface.StagingStorageProvider],
	id, committedBlocks, committedSize, recordedHead int64,
	path string, state iface.GroupState, create bool) (*Group, error) {
	groupPath := filepath.Join(path, "grp", strconv.FormatInt(id, 32))

	if err := os.MkdirAll(groupPath, 0755); err != nil {
		return nil, xerrors.Errorf("create group directory: %w", err)
	}

	// open jbob

	jbOpenFunc := carlog.Open
	if create {
		jbOpenFunc = carlog.Create
	}

	var stw carlog.CarStorageProvider
	st := staging.Load()
	if st != nil {
		stw = &carStorageWrapper{
			storage: *st,
			group:   id,
		}
	}

	jb, err := jbOpenFunc(stw, filepath.Join(groupPath, "blklog.meta"), groupPath, func(to int64, h []mh.Multihash) error {
		if to < recordedHead {
			return xerrors.Errorf("cannot rewind jbob head to %d, recorded group head is %d", to, recordedHead)
		}

		return index.DropGroup(ctx, h, id)
	})
	if err != nil {
		return nil, xerrors.Errorf("open jbob (grp: %s): %w", groupPath, err)
	}

	g := &Group{
		db:      db,
		index:   index,
		staging: staging,

		jb: jb,

		committedBlocks: committedBlocks,
		committedSize:   committedSize,

		path:  groupPath,
		id:    id,
		state: state,
	}

	if state >= iface.GroupStateOffloaded {
		g.offloaded.Store(1)
	}

	return g, nil
}

func (m *Group) Put(ctx context.Context, b []blocks.Block) (int, error) {
	// NOTE: Put is the only method which writes data to jbob

	if len(b) == 0 {
		return 0, nil
	}

	// jbob writes are not thread safe, take the lock to get serial access
	m.dataLk.Lock()
	defer m.dataLk.Unlock()

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

	m.writeBlocks.Add(int64(writeBlocks))
	m.writeSize.Add(writeSize)

	// backend write

	// 1. (buffer) writes to jbob

	c := make([]mh.Multihash, len(b))
	sz := make([]int32, len(b))
	for i, blk := range b {
		c[i] = blk.Cid().Hash()
		sz[i] = int32(len(blk.RawData()))
	}

	// parallel data(log) / index write; In case of unclean shutdown we may get
	// orphan entries in the top index, but that should be fine - unclean shutdowns
	// generally don't happen a lot, and if we use one of those bad entries, and
	// don't find the data in the correct block group, we'll just try another one.
	// Proper cleanup is also possible, but very expensive as it requires scanning of
	// all the data.
	// tldr we do stuff in parallel because more speed good
	eg := new(errgroup.Group)

	eg.Go(func() error {
		err := m.jb.Put(c[:writeBlocks], b[:writeBlocks])
		if err != nil {
			// todo handle properly (abort, close, check disk space / resources, repopen)
			// todo docrement inflight?
			return xerrors.Errorf("writing to jbob: %w", err)
		}

		return nil
	})

	eg.Go(func() error {
		// 3. write top-level index (before we update group head so replay is possible, before jbob commit so that it's faster)
		//    missed, uncommitted jbob writes should be ignored.
		// ^ TODO: Test this commit edge case
		// TODO: Async index queue
		err := m.index.AddGroup(ctx, c[:writeBlocks], sz[:writeBlocks], m.id)
		if err != nil {
			// todo handle properly (abort, close, check disk space / resources, repopen)
			return xerrors.Errorf("writing index: %w", err)
		}

		return nil
	})

	if err := eg.Wait(); err != nil {
		return 0, xerrors.Errorf("data/index write: %w", err)
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
	m.dataLk.Lock()
	defer m.dataLk.Unlock()

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

func (m *Group) View(ctx context.Context, c []mh.Multihash, cb func(cidx int, found bool, data []byte)) error {
	m.readers.Add(1)
	defer m.readers.Done()

	if m.offloaded.Load() != 0 {
		return ErrOffloaded
	}

	// right now we just read from jbob

	// View is thread safe
	return m.jb.View(c, func(cidx int, found bool, data []byte) error {
		if !found {
			cb(cidx, false, nil)
			return nil
		}

		m.readBlocks.Add(1)
		m.readSize.Add(int64(len(data)))

		cb(cidx, true, data)
		return nil
	})
}

func (m *Group) Close() error {
	if err := m.Sync(context.Background()); err != nil {
		return err
	}

	m.dataLk.Lock()
	defer m.dataLk.Unlock()

	err := m.jb.Close()
	// todo mark as closed
	return err
}

// returns car size and root cid
func (m *Group) writeCar(w io.Writer) (int64, cid.Cid, error) {
	m.readers.Add(1)
	defer m.readers.Done()

	if m.offloaded.Load() != 0 {
		return 0, cid.Undef, ErrOffloaded
	}

	// writeCar is thread safe
	return m.jb.WriteCar(w)
}

func (m *Group) hashSample() ([]mh.Multihash, error) {
	// hashSample is thread safe
	return m.jb.HashSample()
}

type carStorageWrapper struct {
	storage iface.StagingStorageProvider
	group   iface.GroupKey
}

func (c *carStorageWrapper) Has(ctx context.Context) (bool, error) {
	return c.storage.HasCar(ctx, c.group)
}

func (c *carStorageWrapper) ReadAt(p []byte, off int64) (n int, err error) {
	rc, err := c.storage.ReadCar(context.TODO(), c.group, off, int64(len(p)))
	if err != nil {
		return 0, err
	}

	n, err = io.ReadFull(rc, p)
	cerr := rc.Close()

	if err != nil {
		return n, err
	}

	return n, cerr
}

func (c *carStorageWrapper) Upload(ctx context.Context, size int64, src func(writer io.Writer) error) error {
	return c.storage.Upload(ctx, c.group, size, src)
}

var _ carlog.CarStorageProvider = &carStorageWrapper{}

func (m *Group) WrapStorageProvider() carlog.CarStorageProvider {
	p := m.staging.Load()
	if p == nil {
		return nil
	}

	return &carStorageWrapper{
		storage: *p,
		group:   m.id,
	}
}
