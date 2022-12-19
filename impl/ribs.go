package impl

import (
	"context"
	"database/sql"
	iface "github.com/magik6k/carsplit/ribs"
	_ "github.com/mattn/go-sqlite3"
	mh "github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
	"path/filepath"
	"sync"
)

const dbSchema = `

/* groups */

create table if not exists groups
(
    id        integer not null
        constraint groups_pk
            primary key autoincrement,
    blocks      integer not null,
    bytes integer not null,
    writable     integer not null
);

create index if not exists groups_id_index
    on groups (id);

create index if not exists groups_writable_index
    on groups (writable);

/* jbobs */
create table if not exists g_jbob
(
    "group"       integer not null
        constraint g_jbob_pk
            primary key
        constraint g_jbob_groups_id_fk
            references groups,
    recorded_head integer)
    without rowid;

create index if not exists g_jbob_group_index
    on g_jbob ("group");

/* top level index */

create table if not exists "index"
(
    hash    BLOB not null,
    "group" integer
    constraint index_groups_id_fk
        references groups,
    constraint index_pk
        primary key (hash, "group") on conflict ignore
)
    without rowid;

create index if not exists index_group_index
    on "index" ("group");

create index if not exists index_hash_index
    on "index" (hash);

`

func Open(root string) (iface.RIBS, error) {
	db, err := sql.Open("sqlite3", filepath.Join(root, "store.db"))
	if err != nil {
		return nil, xerrors.Errorf("open db: %w", err)
	}

	_, err = db.Exec(dbSchema)
	if err != nil {
		return nil, xerrors.Errorf("exec schema: %w", err)
	}

	return &ribs{
		root:  root,
		db:    db,
		index: NewIndex(db),

		writableGroups: make(map[iface.GroupKey]*Group),
		openGroups:     make(map[iface.GroupKey]*Group),
	}, nil
}

type ribs struct {
	root string

	db    *sql.DB
	index iface.Index

	lk sync.Mutex

	openGroups     map[int64]*Group
	writableGroups map[int64]*Group
}

func (r *ribs) withWritableGroup(prefer iface.GroupKey, cb func(group *Group) error) (selectedGroup iface.GroupKey, err error) {
	r.lk.Lock()
	defer r.lk.Unlock()

	defer func() {
		if err != nil || selectedGroup == iface.UndefGroupKey {
			return
		}
		// if the group was filled, drop it from writableGroups
		if !r.writableGroups[selectedGroup].writable {
			delete(r.writableGroups, selectedGroup)
		}
	}()

	// todo prefer
	for g, grp := range r.writableGroups {
		return g, cb(grp)
	}

	// no writable groups, try to open one

	selectedGroup = iface.UndefGroupKey
	{
		res, err := r.db.Query("select id, blocks, bytes, writable from groups where writable = 1")
		if err != nil {
			return iface.UndefGroupKey, xerrors.Errorf("finding writable groups: %w", err)
		}

		var blocks int64
		var bytes int64
		var writable bool

		for res.Next() {
			err := res.Scan(&selectedGroup, &blocks, &bytes, &writable)
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
			g, err := OpenGroup(r.db, r.index, selectedGroup, bytes, blocks, r.root, true, false)
			if err != nil {
				return iface.UndefGroupKey, xerrors.Errorf("opening group: %w", err)
			}

			r.writableGroups[selectedGroup] = g
			r.openGroups[selectedGroup] = g
			return selectedGroup, cb(g)
		}
	}

	// no writable groups, create one

	err = r.db.QueryRow("insert into groups (blocks, bytes, writable) values (0, 0, 1) returning id").Scan(&selectedGroup)
	if err != nil {
		return iface.UndefGroupKey, xerrors.Errorf("creating group entry: %w", err)
	}

	g, err := OpenGroup(r.db, r.index, selectedGroup, 0, 0, r.root, true, true)
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

	res, err := r.db.Query("select blocks, bytes, writable from groups where writable = 1 and id = ?", group)
	if err != nil {
		return xerrors.Errorf("finding writable groups: %w", err)
	}

	var blocks int64
	var bytes int64
	var writable bool
	var found bool

	for res.Next() {
		err := res.Scan(&blocks, &bytes, &writable)
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

	g, err := OpenGroup(r.db, r.index, group, bytes, blocks, r.root, writable, false)
	if err != nil {
		return xerrors.Errorf("opening group: %w", err)
	}

	if writable {
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

func (r *ribBatch) Put(ctx context.Context, c []mh.Multihash, datas [][]byte) error {
	if len(c) != len(datas) {
		return xerrors.Errorf("cids and data lengths mismatch")
	}

	// todo filter blocks that already exist
	var done int
	for done < len(c) {
		gk, err := r.r.withWritableGroup(r.currentWriteTarget, func(g *Group) error {
			wrote, err := g.Put(ctx, c[done:], datas[done:])
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

func (r *ribBatch) Unlink(ctx context.Context, c []mh.Multihash) error {
	//TODO implement me
	panic("implement me")
}

func (r *ribBatch) Flush(ctx context.Context) error {
	// noop for now, group puts are sync currently
	return nil
}

var _ iface.RIBS = &ribs{}
