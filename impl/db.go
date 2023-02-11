package impl

import (
	"context"
	"database/sql"
	iface "github.com/lotus-web3/ribs"
	"golang.org/x/xerrors"
	"path/filepath"
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

/* SP tracker */
create table if not exists providers (
    id integer not null constraint providers_pk primary key,
    
    ping_ok integer not null,
    
    boost_deals integer not null,
    booster_http integer not null,
    booster_bitswap integer not null,
    
    indexed_success integer not null,
    indexed_fail integer not null,
    
    deal_attempts integer not null,
    deal_success integer not null,
    deal_fail integer not null,
    
    retrprobe_success integer not null,
    retrprobe_fail integer not null,
    retrprobe_blocks integer not null,
    retrprobe_bytes integer not null
);

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

type ribsDB struct {
	db *sql.DB
}

func openRibsDB(root string) (*ribsDB, error) {
	db, err := sql.Open("sqlite3", filepath.Join(root, "store.db"))
	if err != nil {
		return nil, xerrors.Errorf("open db: %w", err)
	}

	_, err = db.Exec(dbSchema)
	if err != nil {
		return nil, xerrors.Errorf("exec schema: %w", err)
	}

	return &ribsDB{
		db: db,
	}, nil
}

func (r *ribsDB) GetWritableGroup() (selected iface.GroupKey, blocks, bytes int64, state iface.GroupState, err error) {
	res, err := r.db.Query("select id, blocks, bytes, g_state from groups where g_state = 0")
	if err != nil {
		return 0, 0, 0, 0, xerrors.Errorf("finding writable groups: %w", err)
	}

	selectedGroup := iface.UndefGroupKey

	for res.Next() {
		err := res.Scan(&selectedGroup, &blocks, &bytes, &state)
		if err != nil {
			return 0, 0, 0, 0, xerrors.Errorf("scanning group: %w", err)
		}

		break
	}

	if err := res.Err(); err != nil {
		return 0, 0, 0, 0, xerrors.Errorf("iterating groups: %w", err)
	}
	if err := res.Close(); err != nil {
		return 0, 0, 0, 0, xerrors.Errorf("closing group iterator: %w", err)
	}

	return selectedGroup, blocks, bytes, state, nil
}

func (r *ribsDB) CreateGroup() (out iface.GroupKey, err error) {
	err = r.db.QueryRow("insert into groups (blocks, bytes, g_state, jb_recorded_head) values (0, 0, 0, 0) returning id").Scan(&out)
	if err != nil {
		return iface.UndefGroupKey, xerrors.Errorf("creating group entry: %w", err)
	}

	return
}

func (r *ribsDB) OpenGroup(gid iface.GroupKey) (blocks, bytes int64, state iface.GroupState, err error) {
	res, err := r.db.Query("select blocks, bytes, g_state from groups where id = ?", gid)
	if err != nil {
		return 0, 0, 0, xerrors.Errorf("finding writable groups: %w", err)
	}

	var found bool

	for res.Next() {
		err := res.Scan(&blocks, &bytes, &state)
		if err != nil {
			return 0, 0, 0, xerrors.Errorf("scanning group: %w", err)
		}

		found = true

		break
	}

	if err := res.Err(); err != nil {
		return 0, 0, 0, xerrors.Errorf("iterating groups: %w", err)
	}
	if err := res.Close(); err != nil {
		return 0, 0, 0, xerrors.Errorf("closing group iterator: %w", err)
	}
	if !found {
		return 0, 0, 0, xerrors.Errorf("group %d not found", gid)
	}

	return blocks, bytes, state, nil
}

func (r *ribsDB) SetGroupHead(ctx context.Context, id iface.GroupKey, state iface.GroupState, commBlk, commSz, at int64) error {
	_, err := r.db.ExecContext(ctx, `begin transaction;
		update groups set blocks = ?, bytes = ?, g_state = ?, jb_recorded_head = ? where id = ?;
		commit;`, commBlk, commSz, state, at, id)
	if err != nil {
		return xerrors.Errorf("update group head: %w", err)
	}

	return nil
}

func (r *ribsDB) SetGroupState(ctx context.Context, id iface.GroupKey, state iface.GroupState) error {
	_, err := r.db.ExecContext(ctx, `update groups set g_state = ? where id = ?;`, state, id)
	if err != nil {
		return xerrors.Errorf("update group state: %w", err)
	}

	return nil
}

/* DIAGNOSTICS */

func (r *ribsDB) Groups() ([]iface.GroupKey, error) {
	res, err := r.db.Query("select id from groups")
	if err != nil {
		return nil, xerrors.Errorf("listing groups: %w", err)
	}

	var groups []iface.GroupKey
	for res.Next() {
		var id int64
		err := res.Scan(&id)
		if err != nil {
			return nil, xerrors.Errorf("scanning group: %w", err)
		}

		groups = append(groups, id)
	}

	if err := res.Err(); err != nil {
		return nil, xerrors.Errorf("iterating groups: %w", err)
	}

	if err := res.Close(); err != nil {
		return nil, xerrors.Errorf("closing group iterator: %w", err)
	}

	return groups, nil
}

func (r *ribsDB) GroupMeta(gk iface.GroupKey) (iface.GroupMeta, error) {
	res, err := r.db.Query("select blocks, bytes, g_state from groups where id = ?", gk)
	if err != nil {
		return iface.GroupMeta{}, xerrors.Errorf("getting group meta: %w", err)
	}

	var blocks int64
	var bytes int64
	var state iface.GroupState
	var found bool

	for res.Next() {
		err := res.Scan(&blocks, &bytes, &state)
		if err != nil {
			return iface.GroupMeta{}, xerrors.Errorf("scanning group: %w", err)
		}

		found = true

		break
	}

	if err := res.Err(); err != nil {
		return iface.GroupMeta{}, xerrors.Errorf("iterating groups: %w", err)
	}

	if err := res.Close(); err != nil {
		return iface.GroupMeta{}, xerrors.Errorf("closing group iterator: %w", err)
	}

	if !found {
		return iface.GroupMeta{}, xerrors.Errorf("group %d not found", gk)
	}

	return iface.GroupMeta{
		State: state,

		MaxBlocks: maxGroupBlocks,
		MaxBytes:  maxGroupSize,

		Blocks: blocks,
		Bytes:  bytes,
	}, nil
}
