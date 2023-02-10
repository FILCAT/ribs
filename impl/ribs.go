package impl

import (
	"context"
	"database/sql"
	blocks "github.com/ipfs/go-block-format"
	logging "github.com/ipfs/go-log/v2"
	iface "github.com/lotus-web3/ribs"
	_ "github.com/mattn/go-sqlite3"
	mh "github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
	"path/filepath"
	"sync"
)

var log = logging.Logger("ribs")

const dbSchema = `

/* groups */

create table if not exists groups
(
    id        integer not null
        constraint groups_pk
            primary key autoincrement,
    blocks      integer not null,
    bytes integer not null,    
    /* States
	 * 0 - writable
     * 1 - full
     * 2 - bsst exists
     * 3 - level index dropped
     * 4 - vrcar done
     * 5 - has commp
     * 6 - deals started
     * 7 - deals done
     * 8 - offloaded
     */
    g_state     integer not null,
    
    /* jbob */
    jb_recorded_head integer not null
);

create index if not exists groups_id_index
    on groups (id);

create index if not exists groups_g_state_index
    on groups (g_state);

/* top level index */

create table if not exists top_index
(
    hash    BLOB not null,
    group_id integer
    constraint index_groups_id_fk
        references groups,
    constraint index_pk
        primary key (hash, group_id) on conflict ignore
)
    without rowid;

create index if not exists index_group_index
    on top_index (group_id);

create index if not exists index_hash_index
    on top_index (hash);

`

type openOptions struct {
	workerGate chan struct{} // for testing
}

type OpenOption func(*openOptions)

func WithWorkerGate(gate chan struct{}) OpenOption {
	return func(o *openOptions) {
		o.workerGate = gate
	}
}

func Open(root string, opts ...OpenOption) (iface.RIBS, error) {
	db, err := sql.Open("sqlite3", filepath.Join(root, "store.db"))
	if err != nil {
		return nil, xerrors.Errorf("open db: %w", err)
	}

	_, err = db.Exec(dbSchema)
	if err != nil {
		return nil, xerrors.Errorf("exec schema: %w", err)
	}

	opt := &openOptions{
		workerGate: make(chan struct{}),
	}
	close(opt.workerGate)

	for _, o := range opts {
		o(opt)
	}

	r := &ribs{
		root:  root,
		db:    db,
		index: NewIndex(db),

		writableGroups: make(map[iface.GroupKey]*Group),

		// all open groups (including all writable)
		openGroups: make(map[iface.GroupKey]*Group),

		tasks: make(chan task, 16),

		close:  make(chan struct{}),
		closed: make(chan struct{}),
	}

	// todo resume tasks

	go r.groupWorker(opt.workerGate)

	return r, nil
}

func (r *ribs) groupWorker(gate <-chan struct{}) {
	for {
		<-gate
		select {
		case task := <-r.tasks:
			r.workerExecTask(task)
		case <-r.close:
			close(r.closed)
			return
		}
	}
}

func (r *ribs) workerExecTask(task task) {
	switch task.tt {
	case taskTypeFinalize:
		r.lk.Lock()
		defer r.lk.Unlock()

		g, ok := r.openGroups[task.group]
		if !ok {
			log.Errorw("group not open", "group", task.group, "task", task)
			return
		}

		err := g.Finalize(context.TODO())
		if err != nil {
			log.Errorf("finalizing group: %s", err)
		}
	}
}

type taskType int

const (
	taskTypeFinalize taskType = iota
)

type task struct {
	tt    taskType
	group iface.GroupKey
}

type ribs struct {
	root string

	// todo hide this db behind an interface
	db    *sql.DB
	index iface.Index

	lk sync.Mutex

	close  chan struct{}
	closed chan struct{}

	tasks chan task

	openGroups     map[int64]*Group
	writableGroups map[int64]*Group
}

func (r *ribs) Close() error {
	close(r.close)
	<-r.closed

	// todo close all open groups

	return nil
}

func (r *ribs) withWritableGroup(prefer iface.GroupKey, cb func(group *Group) error) (selectedGroup iface.GroupKey, err error) {
	r.lk.Lock()
	defer r.lk.Unlock()

	defer func() {
		if err != nil || selectedGroup == iface.UndefGroupKey {
			return
		}
		// if the group was filled, drop it from writableGroups and start finalize
		if r.writableGroups[selectedGroup].state != iface.GroupStateWritable {
			delete(r.writableGroups, selectedGroup)

			r.tasks <- task{
				tt:    taskTypeFinalize,
				group: selectedGroup,
			}
		}
	}()

	// todo prefer
	for g, grp := range r.writableGroups {
		return g, cb(grp)
	}

	// no writable groups, try to open one

	selectedGroup = iface.UndefGroupKey
	{
		res, err := r.db.Query("select id, blocks, bytes, g_state from groups where g_state = 0")
		if err != nil {
			return iface.UndefGroupKey, xerrors.Errorf("finding writable groups: %w", err)
		}

		var blocks int64
		var bytes int64
		var state iface.GroupState

		for res.Next() {
			err := res.Scan(&selectedGroup, &blocks, &bytes, &state)
			if err != nil {
				return iface.UndefGroupKey, xerrors.Errorf("scanning group: %w", err)
			}

			break
		}

		if err := res.Err(); err != nil {
			return iface.UndefGroupKey, xerrors.Errorf("iterating groups: %w", err)
		}
		if err := res.Close(); err != nil {
			return iface.UndefGroupKey, xerrors.Errorf("closing group iterator: %w", err)
		}

		if selectedGroup != iface.UndefGroupKey {
			g, err := OpenGroup(r.db, r.index, selectedGroup, bytes, blocks, r.root, state, false)
			if err != nil {
				return iface.UndefGroupKey, xerrors.Errorf("opening group: %w", err)
			}

			r.writableGroups[selectedGroup] = g
			r.openGroups[selectedGroup] = g
			return selectedGroup, cb(g)
		}
	}

	// no writable groups, create one

	err = r.db.QueryRow("insert into groups (blocks, bytes, g_state, jb_recorded_head) values (0, 0, 0, 0) returning id").Scan(&selectedGroup)
	if err != nil {
		return iface.UndefGroupKey, xerrors.Errorf("creating group entry: %w", err)
	}

	g, err := OpenGroup(r.db, r.index, selectedGroup, 0, 0, r.root, iface.GroupStateWritable, true)
	if err != nil {
		return iface.UndefGroupKey, xerrors.Errorf("opening group: %w", err)
	}

	r.writableGroups[selectedGroup] = g
	r.openGroups[selectedGroup] = g
	return selectedGroup, cb(g)
}

func (r *ribs) withReadableGroup(group iface.GroupKey, cb func(group *Group) error) (err error) {
	r.lk.Lock()
	defer r.lk.Unlock()

	// todo prefer
	if r.openGroups[group] != nil {
		return cb(r.openGroups[group])
	}

	// not open, open it

	res, err := r.db.Query("select blocks, bytes, g_state from groups where g_state = 1 and id = ?", group)
	if err != nil {
		return xerrors.Errorf("finding writable groups: %w", err)
	}

	var blocks int64
	var bytes int64
	var state iface.GroupState
	var found bool

	for res.Next() {
		err := res.Scan(&blocks, &bytes, &state)
		if err != nil {
			return xerrors.Errorf("scanning group: %w", err)
		}

		found = true

		break
	}

	if err := res.Err(); err != nil {
		return xerrors.Errorf("iterating groups: %w", err)
	}
	if err := res.Close(); err != nil {
		return xerrors.Errorf("closing group iterator: %w", err)
	}
	if !found {
		return xerrors.Errorf("group %d not found", group)
	}

	g, err := OpenGroup(r.db, r.index, group, bytes, blocks, r.root, state, false)
	if err != nil {
		return xerrors.Errorf("opening group: %w", err)
	}

	if state == iface.GroupStateWritable {
		r.writableGroups[group] = g
	}
	r.openGroups[group] = g
	return cb(g)
}

type ribSession struct {
	r *ribs
}

type ribBatch struct {
	r *ribs

	currentWriteTarget iface.GroupKey

	// todo: use lru
	currentReadTarget iface.GroupKey
}

func (r *ribs) Session(ctx context.Context) iface.Session {
	return &ribSession{
		r: r,
	}
}

func (r *ribSession) View(ctx context.Context, c []mh.Multihash, cb func(cidx int, data []byte)) error {
	done := map[int]struct{}{}
	byGroup := map[iface.GroupKey][]int{}

	err := r.r.index.GetGroups(ctx, c, func(i [][]iface.GroupKey) (bool, error) {
		for cidx, groups := range i {
			if _, ok := done[cidx]; ok {
				continue
			}
			done[cidx] = struct{}{}

			for _, g := range groups {
				if g == iface.UndefGroupKey {
					continue
				}

				byGroup[g] = append(byGroup[g], cidx)
			}
		}

		return len(done) != len(c), nil
	})
	if err != nil {
		return err
	}

	for g, cidxs := range byGroup {
		toGet := make([]mh.Multihash, len(cidxs))
		for i, cidx := range cidxs {
			toGet[i] = c[cidx]
		}

		err := r.r.withReadableGroup(g, func(g *Group) error {
			return g.View(ctx, toGet, func(cidx int, data []byte) {
				cb(cidxs[cidx], data)
			})
		})
		if err != nil {
			return xerrors.Errorf("with readable group: %w", err)
		}
	}

	return nil
}

func (r *ribSession) Batch(ctx context.Context) iface.Batch {
	return &ribBatch{
		r:                  r.r,
		currentWriteTarget: iface.UndefGroupKey,
	}
}

func (r *ribBatch) Put(ctx context.Context, b []blocks.Block) error {
	// todo filter blocks that already exist
	var done int
	for done < len(b) {
		gk, err := r.r.withWritableGroup(r.currentWriteTarget, func(g *Group) error {
			wrote, err := g.Put(ctx, b[done:])
			if err != nil {
				return err
			}
			done += wrote
			return nil
		})
		if err != nil {
			return xerrors.Errorf("write to group: %w", err)
		}

		r.currentWriteTarget = gk
	}

	return nil
}

func (r *ribSession) Has(ctx context.Context, c []mh.Multihash) ([]bool, error) {
	//TODO implement me
	panic("implement me")
}

func (r *ribSession) GetSize(ctx context.Context, c []mh.Multihash) ([]int64, error) {
	//TODO implement me
	panic("implement me")
}

func (r *ribBatch) Unlink(ctx context.Context, c []mh.Multihash) error {
	//TODO implement me
	panic("implement me")
}

func (r *ribBatch) Flush(ctx context.Context) error {
	// noop for now, group puts are sync currently
	return nil
}

var _ iface.RIBS = &ribs{}
