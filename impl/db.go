package impl

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	iface "github.com/lotus-web3/ribs"
	"golang.org/x/xerrors"
	"path/filepath"
)

const mFil = 1_000_000_000_000_000

var (
	maxVerifPrice float64 = 0

	// 2 mFil/gib/mo is roughly cloud cost currently
	maxPrice float64 = (0 * mFil) / 2 / 60 / 24 / 30.436875

	// piece size range ribs is aiming for
	minPieceSize = 4 << 30
	maxPieceSize = 8 << 30
)

var pragmas = []string{
	"PRAGMA synchronous = normal",
	"PRAGMA temp_store = memory",
	"PRAGMA mmap_size = 30000000000",
	"PRAGMA page_size = 32768",
	/*	"PRAGMA auto_vacuum = NONE",
		"PRAGMA automatic_index = OFF",*/
	"PRAGMA journal_mode = WAL",
	"PRAGMA read_uncommitted = ON",
}

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
    jb_recorded_head integer not null,
    
    /* vrcar */
    piece_size integer,
    commp blob,
    car_size integer,
    root blob
);

create index if not exists groups_id_index
    on groups (id);

create index if not exists groups_g_state_index
    on groups (g_state);

/* deals */
create table if not exists deals (
    uuid text not null constraint deals_pk primary key,

    client_addr text not null,
    provider_addr integer not null,

    group_id integer not null,
    price_afil_gib_epoch integer not null,
    verified integer not null,
    keep_unsealed integer not null,

    start_epoch integer not null,
    end_epoch integer not null,

    data_sent integer not null default 0,
    deal_id integer,
    active integer not null default 0,

    /* retrieval checks */
    retrieval_probes_started integer not null default 0,
    retrieval_probes_success integer not null default 0,
    retrieval_probes_fail integer not null default 0
);

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

    retrprobe_success integer not null default 0,
    retrprobe_fail integer not null default 0,
    retrprobe_blocks integer not null default 0,
    retrprobe_bytes integer not null default 0,
    
    ask_ok integer not null default 0,
    ask_price integer not null default 0,
    ask_verif_price integer not null default 0,
    ask_min_piece_size integer not null default 0,
    ask_max_piece_size integer not null default 0
);

create view if not exists good_providers_view as 
	select id, ping_ok, boost_deals, booster_http, booster_bitswap,
       indexed_success, indexed_fail,
       retrprobe_success, retrprobe_fail, retrprobe_blocks, retrprobe_bytes,
       ask_price, ask_verif_price, ask_min_piece_size, ask_max_piece_size
    from providers where in_market = 1 and ping_ok = 1 and ask_ok = 1 and ask_verif_price <= %f and ask_price <= %f and ask_min_piece_size <= %d and ask_max_piece_size >= %d
    order by (booster_bitswap+booster_http) asc, boost_deals asc, id desc;

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

	for _, pragma := range pragmas {
		_, err = db.Exec(pragma)
		if err != nil {
			return nil, xerrors.Errorf("exec pragma: %w", err)
		}
	}

	_, err = db.Exec(fmt.Sprintf(dbSchema, maxVerifPrice, maxPrice, minPieceSize, maxPieceSize))
	if err != nil {
		return nil, xerrors.Errorf("exec schema: %w", err)
	}

	return &ribsDB{
		db: db,
	}, nil
}

func (r *ribsDB) ReachableProviders() []iface.ProviderMeta {
	res, err := r.db.Query(`select id, ping_ok, boost_deals, booster_http, booster_bitswap,
       indexed_success, indexed_fail,
       retrprobe_success, retrprobe_fail, retrprobe_blocks, retrprobe_bytes,
       ask_price, ask_verif_price, ask_min_piece_size, ask_max_piece_size
    from good_providers_view`)

	if err != nil {
		log.Errorw("querying providers", "error", err)
		return nil
	}

	out := make([]iface.ProviderMeta, 0)

	for res.Next() {
		var pm iface.ProviderMeta
		err := res.Scan(&pm.ID, &pm.PingOk, &pm.BoostDeals, &pm.BoosterHttp, &pm.BoosterBitswap,
			&pm.IndexedSuccess, &pm.IndexedFail, // &pm.DealAttempts, &pm.DealSuccess, &pm.DealFail,
			&pm.RetrProbeSuccess, &pm.RetrProbeFail, &pm.RetrProbeBlocks, &pm.RetrProbeBytes,
			&pm.AskPrice, &pm.AskVerifiedPrice, &pm.AskMinPieceSize, &pm.AskMaxPieceSize)
		if err != nil {
			log.Errorw("scanning provider", "error", err)
			return nil
		}

		out = append(out, pm)
	}

	if err := res.Err(); err != nil {
		log.Errorw("scanning providers", "error", err)
		return nil
	}
	if err := res.Close(); err != nil {
		log.Errorw("closing providers", "error", err)
		return nil
	}

	return out
}

func (r *ribsDB) SelectDealProviders(group iface.GroupKey) ([]int64, error) {
	// only reachable, with boost_deals, only ones that don't have deals for this group
	// 6 at random
	// 2 of them with booster_http
	// 2 of them with booster_bitswap

	var withHttp []int64
	var withBitswap []int64
	var random []int64

	res, err := r.db.Query(`select id from good_providers_view where id not in (select provider_addr from deals where group_id = ?) order by random() limit 6`, group)
	if err != nil {
		return nil, xerrors.Errorf("querying providers: %w", err)
	}

	for res.Next() {
		var id int64
		err := res.Scan(&id)
		if err != nil {
			return nil, xerrors.Errorf("scanning provider: %w", err)
		}

		random = append(random, id)
	}

	if err := res.Err(); err != nil {
		return nil, xerrors.Errorf("iterating providers: %w", err)
	}

	if err := res.Close(); err != nil {
		return nil, xerrors.Errorf("closing providers: %w", err)
	}

	res, err = r.db.Query(`select id from good_providers_view where id not in (select provider_addr from deals where group_id = ?) and booster_http = 1 order by random() limit 2`, group)
	if err != nil {
		return nil, xerrors.Errorf("querying providers: %w", err)
	}

	for res.Next() {
		var id int64
		err := res.Scan(&id)
		if err != nil {
			return nil, xerrors.Errorf("scanning provider: %w", err)
		}

		withHttp = append(withHttp, id)
	}

	if err := res.Err(); err != nil {
		return nil, xerrors.Errorf("iterating providers: %w", err)
	}

	if err := res.Close(); err != nil {
		return nil, xerrors.Errorf("closing providers: %w", err)
	}

	res, err = r.db.Query(`select id from good_providers_view where id not in (select provider_addr from deals where group_id = ?) and booster_bitswap = 1 order by random() limit 2`, group)
	if err != nil {
		return nil, xerrors.Errorf("querying providers: %w", err)
	}

	for res.Next() {
		var id int64
		err := res.Scan(&id)
		if err != nil {
			return nil, xerrors.Errorf("scanning provider: %w", err)
		}

		withBitswap = append(withBitswap, id)
	}

	if err := res.Err(); err != nil {
		return nil, xerrors.Errorf("iterating providers: %w", err)
	}

	if err := res.Close(); err != nil {
		return nil, xerrors.Errorf("closing providers: %w", err)
	}

	out := make([]int64, 0, 6)
	out = append(out, withHttp...)
	out = append(out, withBitswap...)
	out = append(out, random...)

	// dedup
	seen := make(map[int64]struct{})
	for _, id := range out {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
	}

	out = make([]int64, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}

	// trim to 5
	if len(out) > 5 {
		out = out[:5]
	}

	fmt.Printf("SELECTED PROVIDERS: %#v\n", out)

	return out, nil
}

type dbDealInfo struct {
	DealUUID string
	GroupID  iface.GroupKey

	ClientAddr   string
	ProviderAddr int64

	PricePerEpoch int64
	Verified      bool
	KeepUnsealed  bool

	StartEpoch abi.ChainEpoch
	EndEpoch   abi.ChainEpoch
}

func (r *ribsDB) StoreDeal(d dbDealInfo) error {
	// also increment deal_attempts for the provider

	_, err := r.db.Exec(`insert into deals (uuid, client_addr, provider_addr, group_id, price_afil_gib_epoch, verified, keep_unsealed, start_epoch, end_epoch) values
                                   (?, ?, ?, ?, ?, ?, ?, ?, ?)`, d.DealUUID, d.ClientAddr, d.ProviderAddr, d.GroupID, d.PricePerEpoch, d.Verified, d.KeepUnsealed, d.StartEpoch, d.EndEpoch)
	if err != nil {
		return xerrors.Errorf("inserting deal: %w", err)
	}

	return nil
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

func (r *ribsDB) GroupStates() (gs map[iface.GroupKey]iface.GroupState, err error) {
	res, err := r.db.Query("select id, g_state from groups")
	if err != nil {
		return nil, xerrors.Errorf("finding writable groups: %w", err)
	}

	gs = make(map[iface.GroupKey]iface.GroupState)

	for res.Next() {
		var id iface.GroupKey
		var state iface.GroupState
		err := res.Scan(&id, &state)
		if err != nil {
			return nil, xerrors.Errorf("scanning group: %w", err)
		}

		gs[id] = state
	}

	if err := res.Err(); err != nil {
		return nil, xerrors.Errorf("iterating groups: %w", err)
	}
	if err := res.Close(); err != nil {
		return nil, xerrors.Errorf("closing group iterator: %w", err)
	}

	return gs, nil
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

func (r *ribsDB) SetCommP(ctx context.Context, id iface.GroupKey, state iface.GroupState, commp []byte, paddedPieceSize int64, root cid.Cid, carSize int64) error {
	_, err := r.db.ExecContext(ctx, `update groups set commp = ?, piece_size = ?, root = ?, car_size = ?, g_state = ? where id = ?;`,
		commp[:], paddedPieceSize, root.Bytes(), carSize, state, id)
	if err != nil {
		return xerrors.Errorf("update group commp: %w", err)
	}

	return nil
}

type dealParams struct {
	CommP     []byte
	Root      cid.Cid
	PieceSize int64
	CarSize   int64
}

func (r *ribsDB) GetDealParams(ctx context.Context, id iface.GroupKey) (out dealParams, err error) {
	res, err := r.db.QueryContext(ctx, "select commp, root, piece_size, car_size from groups where id = ?", id)
	if err != nil {
		return dealParams{}, xerrors.Errorf("finding writable groups: %w", err)
	}

	var found bool

	for res.Next() {
		var commp, root []byte
		var pieceSize, carSize int64
		err := res.Scan(&commp, &root, &pieceSize, &carSize)
		if err != nil {
			return dealParams{}, xerrors.Errorf("scanning group: %w", err)
		}

		out.CommP = commp
		_, out.Root, err = cid.CidFromBytes(root)
		out.PieceSize = pieceSize
		out.CarSize = carSize

		found = true

		break
	}

	if err := res.Err(); err != nil {
		return dealParams{}, xerrors.Errorf("iterating groups: %w", err)
	}
	if err := res.Close(); err != nil {
		return dealParams{}, xerrors.Errorf("closing group iterator: %w", err)
	}
	if !found {
		return dealParams{}, xerrors.Errorf("group %d not found", id)
	}

	return out, nil
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

	var dealMeta []iface.DealMeta

	res, err = r.db.Query("select provider_addr from deals where group_id = ?", gk)
	if err != nil {
		return iface.GroupMeta{}, xerrors.Errorf("getting group meta: %w", err)
	}

	for res.Next() {
		var provider int64
		err := res.Scan(&provider)
		if err != nil {
			return iface.GroupMeta{}, xerrors.Errorf("scanning group: %w", err)
		}

		dealMeta = append(dealMeta, iface.DealMeta{
			Provider: provider,
		})
	}

	if err := res.Err(); err != nil {
		return iface.GroupMeta{}, xerrors.Errorf("iterating deals: %w", err)
	}

	if err := res.Close(); err != nil {
		return iface.GroupMeta{}, xerrors.Errorf("closing deals iterator: %w", err)
	}

	return iface.GroupMeta{
		State: state,

		MaxBlocks: maxGroupBlocks,
		MaxBytes:  maxGroupSize,

		Blocks: blocks,
		Bytes:  bytes,

		Deals: dealMeta,
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

func (r *ribsDB) UpdateProviderStorageAsk(provider int64, ask *storagemarket.StorageAsk) error {
	_, err := r.db.Exec(`
	update providers set ask_price = ?, ask_verif_price = ?, ask_min_piece_size = ?, ask_max_piece_size = ?, ask_ok = 1 where id = ?;
	`, ask.Price.String(), ask.VerifiedPrice.String(), ask.MinPieceSize, ask.MaxPieceSize, provider)
	if err != nil {
		return xerrors.Errorf("update provider: %w", err)
	}

	return nil
}
