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
    
    in_market integer not null,
    
    ping_ok integer not null default 0,
    
    boost_deals integer not null default 0,
    booster_http integer not null default 0,
    booster_bitswap integer not null default 0,
    
    indexed_success integer not null default 0,
    indexed_fail integer not null default 0,
    
    deal_attempts integer not null default 0,
    deal_success integer not null default 0,
    deal_fail integer not null default 0,
    
    retrprobe_success integer not null default 0,
    retrprobe_fail integer not null default 0,
    retrprobe_blocks integer not null default 0,
    retrprobe_bytes integer not null default 0
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

func (r *ribsDB) ReachableProviders() []iface.ProviderMeta {
	res, err := r.db.Query("select id, ping_ok, boost_deals, booster_http, booster_bitswap, indexed_success, indexed_fail, deal_attempts, deal_success, deal_fail, retrprobe_success, retrprobe_fail, retrprobe_blocks, retrprobe_bytes from providers where in_market = 1 and ping_ok = 1 order by (booster_bitswap+booster_http) asc , boost_deals asc , id desc")
	if err != nil {
		return nil
	}

	var out []iface.ProviderMeta

	for res.Next() {
		var pm iface.ProviderMeta
		err := res.Scan(&pm.ID, &pm.PingOk, &pm.BoostDeals, &pm.BoosterHttp, &pm.BoosterBitswap, &pm.IndexedSuccess, &pm.IndexedFail, &pm.DealAttempts, &pm.DealSuccess, &pm.DealFail, &pm.RetrProbeSuccess, &pm.RetrProbeFail, &pm.RetrProbeBlocks, &pm.RetrProbeBytes)
		if err != nil {
			return nil
		}

		out = append(out, pm)
	}

	if err := res.Err(); err != nil {
		return nil
	}
	if err := res.Close(); err != nil {
		return nil
	}

	return out
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

func (r *ribsDB) UpsertMarketActors(actors []int64) error {
	/*_, err := r.db.Exec(`
	begin transaction;
	    update providers set in_market = 0 where in_market = 1;
	    insert into providers (address, in_market) values (?, 1) on conflict (address) do update set in_market = 1;
	end transaction;
	`, actors)*/

	tx, err := r.db.Begin()
	if err != nil {
		return xerrors.Errorf("begin transaction: %w", err)
	}

	_, err = tx.Exec("update providers set in_market = 0 where in_market = 1")
	if err != nil {
		if err := tx.Rollback(); err != nil {
			log.Errorw("rollback UpsertMarketActors", "error", err)
		}
		return xerrors.Errorf("reset in_market: %w", err)
	}

	stmt, err := tx.Prepare("insert into providers (id, in_market) values (?, 1) on conflict (id) do update set in_market = 1")
	if err != nil {
		if err := tx.Rollback(); err != nil {
			log.Errorw("rollback UpsertMarketActors", "error", err)
		}
		return xerrors.Errorf("prepare statement: %w", err)
	}

	for _, actor := range actors {
		_, err = stmt.Exec(actor)
		if err != nil {
			if err := tx.Rollback(); err != nil {
				log.Errorw("rollback UpsertMarketActors", "error", err)
			}
			return xerrors.Errorf("insert actor: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return xerrors.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *ribsDB) UpdateProviderProtocols(provider int64, pres providerResult) error {
	_, err := r.db.Exec(`
	update providers set ping_ok = ?, boost_deals = ?, booster_http = ?, booster_bitswap = ? where id = ?;
	`, pres.PingOk, pres.BoostDeals, pres.BoosterHttp, pres.BoosterBitswap, provider)
	if err != nil {
		return xerrors.Errorf("update provider: %w", err)
	}

	return nil
}
