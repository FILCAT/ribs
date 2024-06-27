package rbdeal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/atboosty/ribs/ributil"
	types2 "github.com/filecoin-project/lotus/chain/types"
	"github.com/multiformats/go-multiaddr"

	iface "github.com/atboosty/ribs"
	types "github.com/atboosty/ribs/ributil/boosttypes"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
)

type ribsDB struct {
	db *ributil.RetryDB

	dealSummaryCq *ributil.CachedQuery[iface.DealSummary]
	reachableCq   *ributil.CachedQuery[[]iface.ProviderMeta]

	lastAnalyzed time.Time
}

var pragmas = []string{
	"PRAGMA synchronous = normal",
	"PRAGMA temp_store = memory",
	"PRAGMA mmap_size = 30000000000",
	"PRAGMA page_size = 32768",
	/*	"PRAGMA auto_vacuum = NONE",
		"PRAGMA automatic_index = OFF",*/
	"PRAGMA journal_mode = WAL",
	"PRAGMA read_uncommitted = ON",
	"PRAGMA busy_timeout = 50000",
}

const dbSchema = `
/* deals */
create table if not exists deals (
    uuid text not null constraint deals_pk primary key,
    start_time integer default (strftime('%s','now')) not null,

    client_addr text not null,
    provider_addr integer not null,

    group_id integer not null,
    price_afil_gib_epoch integer not null,
    verified integer not null,
    keep_unsealed integer not null,

    start_epoch integer not null,
    end_epoch integer not null,

    signed_proposal_bytes blob not null,

    deal_id integer,
    deal_pub_ts text,
    sector_start_epoch integer,

    /* deal state */
    proposed integer not null default 0, /* 1 when the deal is successfully proposed */
    published integer not null default 0, /* publish cid is set, and we have validated the message is landed on chain with some finality */
    sealed integer not null default 0, /* deal state SectorStartEpoch set */

    failed integer not null default 0, /* 1 when the deal is unsuccessful for ANY reason */
    rejected integer not null default 0,

    failed_expired integer not null default 0, /* 1 when the deal is failed AND the proposal start has passed TODO */

    error_msg text,

    /* status queries */
    last_state_query integer default 0 not null,
    last_state_query_error text,

    /* data transfer */
    car_transfer_start_time integer,
    car_transfer_attempts integer not null default 0,

    car_transfer_last_end_time integer,
    car_transfer_last_bytes integer,

    /* sp deal state */
    sp_status text, /* boost checkpoint name */
    sp_sealing_status text,
    sp_sig_proposal text,
    sp_pub_msg_cid text,

    sp_recv_bytes integer,
    sp_txsize integer, /* todo swap for car_size in group? */

    /* market deal state checks */
    last_deal_state_check integer not null default 0,

    /* retrieval checks */
    last_retrieval_check integer not null default 0,
    last_retrieval_check_success integer not null default 0,
    retrieval_probes_success integer not null default 0,
    retrieval_probes_fail integer not null default 0,

    retrieval_probe_prev_error text,

    retrieval_probe_prev_ms integer,
    retrieval_probe_prev_ttfb_ms integer
);

CREATE TABLE IF NOT EXISTS deals_archive AS SELECT * FROM deals WHERE 0;

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
    ask_max_piece_size integer not null default 0,

    addr_info_graphsync text,
    addr_info_bitswap text,
    addr_info_http text
);

create table if not exists offloads_s3
(
    group_id integer not null
        constraint offloads_s3_pk
            primary key
);

create table if not exists repairs
(
    group_id          integer           not null
        constraint repairs_pk
            primary key,
    retrievable_deals integer           not null,
    worker            integer,
    last_attempt      integer default 0 not null
);

drop view if exists sp_deal_stats_view;
drop view if exists sp_retr_stats_view;
drop view if exists bad_providers_new_reject_view;
drop view if exists good_providers_view;

CREATE VIEW IF NOT EXISTS bad_providers_new_reject_view AS
    SELECT 
        d.provider_addr AS sp_id
    FROM 
        deals d
    WHERE 
        d.start_time >= strftime('%s', 'now', '-2 hours')
    GROUP BY
        d.provider_addr
    HAVING 
        COUNT(*) > 2
        AND COUNT(CASE WHEN d.rejected = 1 THEN 1 ELSE NULL END) = COUNT(*);

CREATE VIEW IF NOT EXISTS sp_deal_stats_view AS
    SELECT
        d.provider_addr AS sp_id,
        COUNT(*) AS total_deals,
        COUNT(CASE WHEN d.published = 1 THEN 1 ELSE NULL END) AS published_deals,
        COUNT(CASE WHEN d.sealed = 1 THEN 1 ELSE NULL END) AS sealed_deals,
        COUNT(CASE WHEN d.failed = 1 THEN 1 ELSE NULL END) AS failed_deals,
        COUNT(CASE WHEN d.rejected = 1 THEN 1 ELSE NULL END) AS rejected_deals,
        CASE
            WHEN COUNT(CASE WHEN d.rejected = 0 THEN 1 ELSE NULL END) >= 4 
                AND COUNT(CASE WHEN d.rejected = 0 THEN 1 ELSE NULL END) * 4 
                    < COUNT(CASE WHEN d.failed = 1 AND d.rejected = 0 THEN 1 ELSE NULL END) * 5 
            THEN 1
            ELSE 0
            END AS failed_all
    FROM
        deals d
            JOIN
        groups g ON d.group_id = g.id
    WHERE
        d.start_time >= strftime('%s', 'now', '-3 days')
    GROUP BY
        d.provider_addr;

CREATE VIEW IF NOT EXISTS sp_retr_stats_view AS
SELECT
    d.provider_addr AS sp_id,
    COUNT(CASE WHEN d.last_retrieval_check < (d.last_retrieval_check_success + 3600*24) THEN 1 ELSE NULL END) AS retrievable_deals,
    COUNT(CASE WHEN d.last_retrieval_check > (d.last_retrieval_check_success + 3600*24) THEN 1 ELSE NULL END) AS unretrievable_deals
FROM
    deals d
WHERE
        d.last_retrieval_check > 0
GROUP BY
    d.provider_addr;

CREATE TABLE IF NOT EXISTS bad_providers_new_reject AS SELECT * FROM bad_providers_new_reject_view WHERE 0;
CREATE TABLE IF NOT EXISTS sp_deal_stats AS SELECT * FROM sp_deal_stats_view WHERE 0;
CREATE TABLE IF NOT EXISTS sp_retr_stats AS SELECT * FROM sp_retr_stats_view WHERE 0;

CREATE TABLE IF NOT EXISTS good_providers (
	id INTEGER PRIMARY KEY,
	ping_ok INTEGER,
	
	boost_deals INTEGER,
	booster_http INTEGER,
	booster_bitswap INTEGER,
	
	indexed_success INTEGER,
	indexed_fail INTEGER,
	
	retrprobe_success INTEGER,
	retrprobe_fail INTEGER,
	retrprobe_blocks INTEGER, 
	retrprobe_bytes INTEGER,
	
	ask_price INTEGER,
	ask_verif_price INTEGER,
	ask_min_piece_size INTEGER,
	ask_max_piece_size INTEGER
);

CREATE INDEX IF NOT EXISTS idx_providers_eligible ON providers(in_market, ping_ok, ask_ok, ask_min_piece_size, ask_max_piece_size);
CREATE INDEX IF NOT EXISTS idx_deals_provider ON deals(provider_addr, group_id, rejected, start_time);
CREATE INDEX IF NOT EXISTS idx_deals_group ON deals(group_id, rejected, start_time);
CREATE INDEX IF NOT EXISTS idx_deals_retrieval ON deals(last_retrieval_check, last_retrieval_check_success);

CREATE TABLE IF NOT EXISTS schema_version (
    version_number INTEGER PRIMARY KEY,
    description TEXT,
    applied_on DATETIME DEFAULT CURRENT_TIMESTAMP
);
`

type schema struct {
	VersionNumber int
	Description   string
	Schema        string
}

var schemas = []schema{
	{
		VersionNumber: 2,
		Description:   "Add sector_number to deals table",
		Schema:        `ALTER TABLE deals ADD COLUMN sector_number INTEGER;`,
	}}

func openRibsDB(root string) (*ribsDB, error) {
	rdb, err := sql.Open("sqlite3", filepath.Join(root, "store.db"))
	if err != nil {
		return nil, xerrors.Errorf("open db: %w", err)
	}

	db := ributil.NewRetryDB(rdb)

	for _, pragma := range pragmas {
		_, err := db.Exec(pragma)
		if err != nil {
			return nil, xerrors.Errorf("exec pragma: %w", err)
		}
	}

	rd := &ribsDB{
		db: db,

		lastAnalyzed: time.Now(),
	}

	return rd, nil
}

var analyzeInterval = 6 * time.Hour

func (r *ribsDB) startDB() error {
	_, err := r.db.Exec(dbSchema)
	if err != nil {
		return xerrors.Errorf("exec schema: %w", err)
	}

	// Apply any pending schema updates
	for i, s := range schemas {
		var version int
		err := r.db.QueryRow("SELECT version_number FROM schema_version WHERE version_number = ?", s.VersionNumber).Scan(&version)
		if err == sql.ErrNoRows {
			_, err = r.db.Exec(s.Schema)
			if err != nil {
				return xerrors.Errorf("exec schema update %d: %w", i, err)
			}

			_, err = r.db.Exec("INSERT INTO schema_version (version_number, description) VALUES (?, ?)", s.VersionNumber, s.Description)
			if err != nil {
				return xerrors.Errorf("insert schema version %d: %w", i, err)
			}
		} else if err != nil {
			return xerrors.Errorf("query schema version %d: %w", i, err)
		}
	}

	r.dealSummaryCq = ributil.NewCachedQuery[iface.DealSummary](1*time.Minute, r.dealSummary)
	r.reachableCq = ributil.NewCachedQuery[[]iface.ProviderMeta](1*time.Minute, r.reachableProviders)

	if err := timeDBOp("refresh_bad_providers_new_reject", r.db, refreshViewTable("bad_providers_new_reject")); err != nil {
		return err
	}
	if err := timeDBOp("refresh_sp_deal_stats", r.db, refreshViewTable("sp_deal_stats")); err != nil {
		return err
	}
	if err := timeDBOp("refresh_sp_retr_stats", r.db, refreshViewTable("sp_retr_stats")); err != nil {
		return err
	}
	if err := timeDBOp("refresh_good_providers", r.db, refreshGoodProviders()); err != nil {
		return err
	}

	go func() {
		for {
			time.Sleep(2 * time.Minute)
			if err := timeDBOp("refresh_bad_providers_new_reject", r.db, refreshViewTable("bad_providers_new_reject")); err != nil {
				continue
			}
			if err := timeDBOp("refresh_sp_deal_stats", r.db, refreshViewTable("sp_deal_stats")); err != nil {
				continue
			}
			if err := timeDBOp("refresh_sp_retr_stats", r.db, refreshViewTable("sp_retr_stats")); err != nil {
				continue
			}
			if err := timeDBOp("refresh_good_providers", r.db, refreshGoodProviders()); err != nil {
				continue
			}

			if time.Since(r.lastAnalyzed) > analyzeInterval {
				_ = timeDBOp("analyze", r.db, func(db *ributil.RetryDB) error {
					_, err := db.Exec("ANALYZE")
					return err
				})
				r.lastAnalyzed = time.Now()
			}
		}
	}()

	return nil
}

func timeDBOp(name string, db *ributil.RetryDB, f func(db *ributil.RetryDB) error) error {
	start := time.Now()
	err := f(db)
	log.Errorw("DB op time", "name", name, "took", time.Since(start), "error", err)
	return err
}

func refreshViewTable(name string) func(db *ributil.RetryDB) error {
	return func(db *ributil.RetryDB) error {
		tempTable := name + "_tmp"
		viewTable := name + "_view"
		targetTable := name

		_, err := db.Exec(`
		CREATE TEMP TABLE ` + tempTable + ` AS SELECT * FROM ` + viewTable + `;
		DELETE FROM ` + targetTable + `;
		INSERT INTO ` + targetTable + ` SELECT * FROM ` + tempTable + `;
		DROP TABLE ` + tempTable + `;`)

		return err
	}
}

func refreshGoodProviders() func(db *ributil.RetryDB) error {
	return func(db *ributil.RetryDB) error {
		_, err := db.Exec(fmt.Sprintf(`
	CREATE TEMP TABLE good_providers_tmp_imm1 AS SELECT p.* FROM providers p
		 LEFT JOIN bad_providers_new_reject bp ON p.id = bp.sp_id
	WHERE p.in_market = 1
		AND p.ping_ok = 1
        AND p.ask_ok = 1
        AND p.ask_min_piece_size <= %d
        AND p.ask_max_piece_size >= %d
		AND bp.sp_id IS NULL;  -- Excludes bad providers

	CREATE TEMP TABLE good_providers_tmp AS SELECT 
        p.id, p.ping_ok, p.boost_deals, p.booster_http, p.booster_bitswap,
        p.indexed_success, p.indexed_fail,
        p.retrprobe_success, p.retrprobe_fail, p.retrprobe_blocks, p.retrprobe_bytes,
        p.ask_price, p.ask_verif_price, p.ask_min_piece_size, p.ask_max_piece_size
    FROM 
        good_providers_tmp_imm1 p
        LEFT JOIN sp_deal_stats ds ON p.id = ds.sp_id
        LEFT JOIN sp_retr_stats rs ON p.id = rs.sp_id
    WHERE
        (ds.failed_all IS NULL OR ds.failed_all = 0)
        AND (rs.unretrievable_deals IS NULL OR (rs.unretrievable_deals <= 1 OR rs.retrievable_deals >= 0.7 * (rs.retrievable_deals + rs.unretrievable_deals) )) /* has up to 1 unretrievable deals, or most are retrievable  */
    ORDER BY
        (p.booster_bitswap + p.booster_http) ASC, p.boost_deals ASC, p.id DESC;

		DROP TABLE good_providers_tmp_imm1;

		DELETE FROM good_providers;
		INSERT INTO good_providers SELECT * FROM good_providers_tmp;
		DROP TABLE good_providers_tmp;`, maxPieceSize, minPieceSize))

		return err
	}
}

func refreshGoodProviders5(db *ributil.RetryDB) error {
	// Query to populate good_providers table
	q := `
    INSERT INTO good_providers
        SELECT 
        p.id, p.ping_ok, p.boost_deals, p.booster_http, p.booster_bitswap,
        p.indexed_success, p.indexed_fail,
        p.retrprobe_success, p.retrprobe_fail, p.retrprobe_blocks, p.retrprobe_bytes,
        p.ask_price, p.ask_verif_price, p.ask_min_piece_size, p.ask_max_piece_size
    FROM 
        providers p
        LEFT JOIN sp_deal_stats_view ds ON p.id = ds.sp_id
        LEFT JOIN sp_retr_stats_view rs ON p.id = rs.sp_id
        LEFT JOIN bad_providers_new_reject_view bp ON p.id = bp.sp_id
    WHERE 
        p.in_market = 1
        AND p.ping_ok = 1
        AND p.ask_ok = 1
        AND p.ask_min_piece_size <= %d
        AND p.ask_max_piece_size >= %d
        AND (ds.failed_all IS NULL OR ds.failed_all = 0)
        AND (rs.unretrievable_deals IS NULL OR (rs.unretrievable_deals <= 1 OR rs.retrievable_deals >= 0.7 * (rs.retrievable_deals + rs.unretrievable_deals) )) /* has up to 1 unretrievable deals, or most are retrievable  */
        AND bp.sp_id IS NULL  -- Excludes bad providers
    ORDER BY
        (p.booster_bitswap + p.booster_http) ASC, p.boost_deals ASC, p.id DESC;
  `

	// Delete existing rows
	if _, err := db.Exec("DELETE FROM good_providers"); err != nil {
		return err
	}

	// Insert refreshed data
	if _, err := db.Exec(fmt.Sprintf(q, maxPieceSize, minPieceSize)); err != nil {
		return err
	}

	// Refresh indexes
	_, err := db.Exec("ANALYZE")

	return err
}

type dealProvider struct {
	id              int64
	ask_price       float64
	ask_verif_price float64
}

func (r *ribsDB) SelectDealProviders(group iface.GroupKey, pieceSize int64, verified bool, maxPrice float64) ([]dealProvider, error) {
	// only reachable, with boost_deals, only ones that don't have deals for this group
	// 6 at random
	// 2 of them with booster_http
	// 2 of them with booster_bitswap

	var withHttp []dealProvider
	var withBitswap []dealProvider
	var random []dealProvider

	res, err := r.db.Query(`select id, ask_price, ask_verif_price from good_providers
									WHERE id NOT IN (
										SELECT provider_addr FROM deals	WHERE group_id = ?
										  AND (rejected = 0 OR (rejected = 1 AND start_time >= strftime('%s', 'now', '-24 hours')))
										  AND (failed = 0 OR (rejected = 0 AND failed = 1 AND  start_time >= strftime('%s', 'now', '-100 hours')))
									) and ask_min_piece_size <= ? and ask_max_piece_size >= ? order by random() limit 15`,
		group, pieceSize, pieceSize)
	if err != nil {
		return nil, xerrors.Errorf("querying providers: %w", err)
	}
	defer res.Close()

	for res.Next() {
		var id dealProvider
		err := res.Scan(&id.id, &id.ask_price, &id.ask_verif_price)
		if err != nil {
			return nil, xerrors.Errorf("scanning provider: %w", err)
		}

		if verified && id.ask_verif_price > maxPrice {
			continue
		} else if !verified && id.ask_price > maxPrice {
			continue
		}

		random = append(random, id)
	}

	if err := res.Err(); err != nil {
		return nil, xerrors.Errorf("iterating providers: %w", err)
	}

	if err := res.Close(); err != nil {
		return nil, xerrors.Errorf("closing providers: %w", err)
	}

	res, err = r.db.Query(`select id, ask_price, ask_verif_price from good_providers
									WHERE id NOT IN (
										SELECT provider_addr FROM deals	WHERE group_id = ?
										  AND (rejected = 0 OR (rejected = 1 AND start_time >= strftime('%s', 'now', '-24 hours')))
										  AND (failed = 0 OR (rejected = 0 AND failed = 1 AND  start_time >= strftime('%s', 'now', '-100 hours')))
									) and booster_http = 1 and ask_min_piece_size <= ? and ask_max_piece_size >= ? order by random() limit 7`, group, pieceSize, pieceSize)
	if err != nil {
		return nil, xerrors.Errorf("querying providers: %w", err)
	}

	for res.Next() {
		var id dealProvider
		err := res.Scan(&id.id, &id.ask_price, &id.ask_verif_price)
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

	res, err = r.db.Query(`select id, ask_price, ask_verif_price from good_providers
									WHERE id NOT IN (
										SELECT provider_addr FROM deals	WHERE group_id = ?
										  AND (rejected = 0 OR (rejected = 1 AND start_time >= strftime('%s', 'now', '-24 hours')))
										  AND (failed = 0 OR (rejected = 0 AND failed = 1 AND  start_time >= strftime('%s', 'now', '-100 hours')))
									) and booster_bitswap = 1 and ask_min_piece_size <= ? and ask_max_piece_size >= ? order by random() limit 7`, group, pieceSize, pieceSize)
	if err != nil {
		return nil, xerrors.Errorf("querying providers: %w", err)
	}

	for res.Next() {
		var id dealProvider
		err := res.Scan(&id.id, &id.ask_price, &id.ask_verif_price)
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

	out := make([]dealProvider, 0, 14)
	out = append(out, withHttp...)
	out = append(out, withBitswap...)
	out = append(out, random...)

	// dedup
	seen := make(map[int64]dealProvider)
	for _, p := range out {
		if _, ok := seen[p.id]; ok {
			continue
		}
		seen[p.id] = p
	}

	out = make([]dealProvider, 0, 9)
	for _, p := range seen {
		out = append(out, p)
		// trim to 9
		if len(out) == 9 {
			break
		}
	}

	return out, nil
}

func (r *ribsDB) ReachableProviders() []iface.ProviderMeta {
	rp, err := r.reachableCq.Get()
	if err != nil {
		log.Errorw("getting reachable providers", "error", err)
	}

	return rp
}

func (r *ribsDB) reachableProviders() ([]iface.ProviderMeta, error) {
	res, err := r.db.Query(`select id, ping_ok, boost_deals, booster_http, booster_bitswap,
       indexed_success, indexed_fail,
       ask_price, ask_verif_price, ask_min_piece_size, ask_max_piece_size
    from providers where in_market=1 and ping_ok=1`)

	if err != nil {
		log.Errorw("querying providers", "error", err)
		return nil, err
	}

	out := make([]iface.ProviderMeta, 0)

	for res.Next() {
		var pm iface.ProviderMeta
		err := res.Scan(&pm.ID, &pm.PingOk, &pm.BoostDeals, &pm.BoosterHttp, &pm.BoosterBitswap,
			&pm.IndexedSuccess, &pm.IndexedFail, // &pm.DealAttempts, &pm.DealSuccess, &pm.DealFail,
			&pm.AskPrice, &pm.AskVerifiedPrice, &pm.AskMinPieceSize, &pm.AskMaxPieceSize)
		if err != nil {
			log.Errorw("scanning provider", "error", err)
			return nil, err
		}

		out = append(out, pm)
	}

	if err := res.Err(); err != nil {
		log.Errorw("scanning providers", "error", err)
		return nil, err
	}
	if err := res.Close(); err != nil {
		log.Errorw("closing providers", "error", err)
		return nil, err
	}

	res, err = r.db.Query(`select provider_addr, count(*),
       sum(case when sealed = 1 then 1 else 0 end),
       sum(case when rejected != 1 and failed = 1 then 1 else 0 end),
       sum(case when rejected = 1 then 1 else 0 end),
       max(start_time) from deals group by provider_addr`)
	if err != nil {
		log.Errorw("querying deals", "error", err)
		return nil, err
	}

	for res.Next() {
		var id int64
		var dealStarted, dealSuccess, dealFail, dealRejected, maxStart int64
		err := res.Scan(&id, &dealStarted, &dealSuccess, &dealFail, &dealRejected, &maxStart)
		if err != nil {
			log.Errorw("scanning deal", "error", err)
			return nil, err
		}

		for i := range out { // todo O(n^2)
			if out[i].ID == id {
				out[i].DealStarted = dealStarted
				out[i].DealSuccess = dealSuccess
				out[i].DealFail = dealFail
				out[i].DealRejected = dealRejected
				out[i].MostRecentDealStart = maxStart
			}
		}
	}

	if err := res.Err(); err != nil {
		log.Errorw("scanning providers", "error", err)
		return nil, err
	}
	if err := res.Close(); err != nil {
		log.Errorw("closing providers", "error", err)
		return nil, err
	}

	res, err = r.db.Query(`select sp_id, retrievable_deals, unretrievable_deals from sp_retr_stats_view`)
	if err != nil {
		log.Errorw("querying deals", "error", err)
		return nil, err
	}
	for res.Next() {
		var id int64
		var retr, unretr int64
		err := res.Scan(&id, &retr, &unretr)
		if err != nil {
			log.Errorw("scanning deal retr stats", "error", err)
			return nil, err
		}

		for i := range out { // todo O(n^2)
			if out[i].ID == id {
				out[i].RetrievDeals = retr
				out[i].UnretrievDeals = unretr
			}
		}
	}

	if err := res.Err(); err != nil {
		log.Errorw("scanning providers", "error", err)
		return nil, err
	}
	if err := res.Close(); err != nil {
		log.Errorw("closing providers", "error", err)
		return nil, err
	}

	sort.SliceStable(out, func(i, j int) bool {
		iRejectAll := out[i].DealRejected == out[i].DealStarted
		jRejectAll := out[j].DealRejected == out[j].DealStarted
		if iRejectAll != jRejectAll {
			return iRejectAll
		}

		iRetrSuccess := (out[i].RetrievDeals+out[i].UnretrievDeals) > 0 && float64(out[i].RetrievDeals) >= 0.7*float64(out[i].RetrievDeals+out[i].UnretrievDeals)
		jRetrSuccess := (out[j].RetrievDeals+out[j].UnretrievDeals) > 0 && float64(out[j].RetrievDeals) >= 0.7*float64(out[j].RetrievDeals+out[j].UnretrievDeals)

		if iRetrSuccess != jRetrSuccess {
			return !iRetrSuccess
		}

		if out[i].DealSuccess == out[j].DealSuccess {
			return out[i].DealStarted < out[j].DealStarted
		}

		return out[i].DealSuccess < out[j].DealSuccess
	})

	return out, nil
}

func (r *ribsDB) GetNonFailedDealCount(group iface.GroupKey) (int, error) {
	var count int
	err := r.db.QueryRow(`select count(*) from deals where group_id = ? and failed = 0 and case when last_retrieval_check > 0 then last_retrieval_check < (last_retrieval_check_success + 3600*24) else 1 end = 1`, group).Scan(&count)
	if err != nil {
		return 0, xerrors.Errorf("querying deal count: %w", err)
	}

	return count, nil
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

	SignedProposalBytes []byte
}

func (r *ribsDB) StoreDealProposal(d dbDealInfo) error {
	_, err := r.db.Exec(`insert into deals (uuid, client_addr, provider_addr, group_id, price_afil_gib_epoch, verified, keep_unsealed, start_epoch, end_epoch, signed_proposal_bytes) values
                                   (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, d.DealUUID, d.ClientAddr, d.ProviderAddr, d.GroupID, d.PricePerEpoch, d.Verified, d.KeepUnsealed, d.StartEpoch, d.EndEpoch, d.SignedProposalBytes)
	if err != nil {
		return xerrors.Errorf("inserting deal: %w", err)
	}

	return nil
}

func (r *ribsDB) StoreSuccessfullyProposedDeal(d dbDealInfo) error {
	proposed := 1

	_, err := r.db.Exec(`update deals set proposed = ? where uuid = ?`,
		proposed, d.DealUUID)
	if err != nil {
		return xerrors.Errorf("updating deal: %w", err)
	}

	return nil
}

func (r *ribsDB) StoreRejectedDeal(duuid string, emsg string, proposed int) error {
	failed, rejected := 1, 1
	state := "Rejected"

	_, err := r.db.Exec(`update deals set failed = ?, rejected = ?, sp_status = ?, error_msg = ?, proposed = ? where uuid = ?`,
		failed, rejected, state, emsg, proposed, duuid)
	if err != nil {
		return xerrors.Errorf("updating deal: %w", err)
	}

	return nil
}

func (r *ribsDB) UpdateSPDealState(id uuid.UUID, stresp *types.DealStatusResponse, qerr error) error {
	now := time.Now().Unix()
	var lastError *string

	if qerr != nil || stresp == nil {
		if qerr != nil {
			errStr := qerr.Error()
			lastError = &errStr
		} else {
			errMsg := "DealStatusResponse is nil"
			lastError = &errMsg
		}

		_, err := r.db.Exec(`update deals set
        last_state_query_error = ?
        where uuid = ?`, lastError, id)
		if err != nil {
			return xerrors.Errorf("update sp tracker: %w", err)
		}
	} else if stresp.DealStatus == nil {
		errMsg := fmt.Sprintf("DealStatus is nil (resp err: '%s')", stresp.Error)

		failed := true

		_, err := r.db.Exec(`update deals set
                 failed = ?,
                 sp_status = ?,
                 error_msg = ?,
                 last_state_query_error = ?,
                 last_state_query = ?
             where uuid = ?`, failed, "Aborted", errMsg, errMsg, now, id)
		if err != nil {
			return xerrors.Errorf("update sp tracker: %w", err)
		}
	} else {
		var pubCid *string
		if stresp.DealStatus.PublishCid != nil {
			s := stresp.DealStatus.PublishCid.String()
			pubCid = &s
		}

		failed := stresp.DealStatus.Error != ""

		_, err := r.db.Exec(`update deals set
        failed = ?,
        sp_status = ?,
        error_msg = ?,
        sp_sealing_status = ?,
        sp_sig_proposal = ?,
        sp_pub_msg_cid = ?,
        sp_recv_bytes = CASE WHEN ? > COALESCE(sp_recv_bytes, 0) THEN ? ELSE sp_recv_bytes END,
        sp_txsize = ?,
        last_state_query = ?,
        last_state_query_error = ?
        where uuid = ?`, failed, stresp.DealStatus.Status, stresp.DealStatus.Error, stresp.DealStatus.SealingStatus,
			stresp.DealStatus.SignedProposalCid.String(), pubCid,
			stresp.NBytesReceived, stresp.NBytesReceived, stresp.TransferSize, now, lastError, id)
		if err != nil {
			return xerrors.Errorf("update sp tracker: %w", err)
		}
	}

	return nil
}

type inactiveDealMeta struct {
	DealUUID     string
	ProviderAddr int64
}

func (r *ribsDB) InactiveDealsToCheck() ([]inactiveDealMeta, error) {
	res, err := r.db.Query(`select uuid, provider_addr from deals where sealed = 0 and failed = 0`) // todo any reason to re-check failed/rejected deals?
	if err != nil {
		return nil, xerrors.Errorf("querying deals: %w", err)
	}
	defer res.Close()

	out := make([]inactiveDealMeta, 0)

	for res.Next() {
		var dm inactiveDealMeta
		err := res.Scan(&dm.DealUUID, &dm.ProviderAddr)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal: %w", err)
		}

		out = append(out, dm)
	}

	return out, nil
}

type publishingDealMeta struct {
	DealUUID     string
	ProviderAddr int64

	Proposal   []byte
	PublishCid string
}

func (r *ribsDB) PublishingDeals() ([]publishingDealMeta, error) {
	res, err := r.db.Query(`select uuid, provider_addr, signed_proposal_bytes, sp_pub_msg_cid from deals where published = 0 and failed = 0 and sp_pub_msg_cid is not null`) // todo any reason to re-check failed/rejected deals?
	if err != nil {
		return nil, xerrors.Errorf("querying deals: %w", err)
	}
	defer res.Close()

	out := make([]publishingDealMeta, 0)

	for res.Next() {
		var dm publishingDealMeta
		err := res.Scan(&dm.DealUUID, &dm.ProviderAddr, &dm.Proposal, &dm.PublishCid)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal: %w", err)
		}

		out = append(out, dm)
	}

	return out, nil
}

func (r *ribsDB) UpdatePublishedDeal(id string, dealID abi.DealID, pubTs types2.TipSetKey) error {
	_, err := r.db.Exec(`update deals set deal_id = ?, deal_pub_ts = ?, published = 1 where uuid = ?`, dealID, pubTs.String(), id)
	if err != nil {
		return xerrors.Errorf("update activated deal: %w", err)
	}

	return nil
}

type publishedDealMeta struct {
	DealUUID     string
	ProviderAddr int64

	Proposal   []byte
	PublishCid string
	DealID     abi.DealID
}

func (r *ribsDB) PublishedDeals() ([]publishedDealMeta, error) {
	res, err := r.db.Query(`select uuid, provider_addr, signed_proposal_bytes, sp_pub_msg_cid, deal_id from deals where published = 1 and sealed = 0 and failed = 0 and sp_pub_msg_cid is not null`) // todo any reason to re-check failed/rejected deals?
	if err != nil {
		return nil, xerrors.Errorf("querying deals: %w", err)
	}
	defer res.Close()

	out := make([]publishedDealMeta, 0)

	for res.Next() {
		var dm publishedDealMeta
		err := res.Scan(&dm.DealUUID, &dm.ProviderAddr, &dm.Proposal, &dm.PublishCid, &dm.DealID)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal: %w", err)
		}

		out = append(out, dm)
	}

	return out, nil
}

func (r *ribsDB) UpdateActivatedDeal(id string, sectorStart abi.ChainEpoch) error {
	_, err := r.db.Exec(`update deals set sector_start_epoch = ?, sealed = 1 where uuid = ?`, sectorStart, id)
	if err != nil {
		return xerrors.Errorf("update activated deal: %w", err)
	}

	return nil
}

func (r *ribsDB) MarkExpiredDeals(currentEpoch int64) error {
	query := `
		UPDATE deals
		SET failed = 1,
			failed_expired = 1,
			published = 0,
			sp_pub_msg_cid = null
		WHERE failed = 0 AND sealed = 0
			AND start_epoch < ?;
	`

	result, err := r.db.Exec(query, currentEpoch, currentEpoch)
	if err != nil {
		return fmt.Errorf("error marking expired deals: %w", err)
	}

	affectedRows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("error getting affected rows: %w", err)
	}

	log.Warnw("Marked expired deals", "affectedRows", affectedRows)

	return nil
}

func (r *ribsDB) GetDealStartEpoch(uuid string) (abi.ChainEpoch, error) {
	var startEpoch abi.ChainEpoch
	err := r.db.QueryRow(`SELECT start_epoch FROM deals WHERE uuid = ?`, uuid).Scan(&startEpoch)
	if err != nil {
		return 0, xerrors.Errorf("getting start_epoch by uuid: %w", err)
	}

	return startEpoch, nil
}

func (r *ribsDB) UpdateExpiredDeal(id string) error {
	_, err := r.db.Exec(`update deals set failed = 1, failed_expired = 1 where uuid = ?`, id)
	if err != nil {
		return xerrors.Errorf("update activated deal: %w", err)
	}

	return nil
}

func (r *ribsDB) DealSummary() (iface.DealSummary, error) {
	return r.dealSummaryCq.Get()
}

func (r *ribsDB) dealSummary() (iface.DealSummary, error) {

	res, err := r.db.Query(`WITH deal_summary AS (
    SELECT
        d.group_id,
        SUM(CASE WHEN d.failed = 0 THEN g.car_size ELSE 0 END) AS total_data_size,
        SUM(CASE WHEN d.failed = 0 THEN g.piece_size ELSE 0 END) AS total_deal_size,
        SUM(CASE WHEN d.failed = 0 AND d.sealed = 1 THEN g.car_size ELSE 0 END) AS stored_data_size,
        SUM(CASE WHEN d.failed = 0 AND d.sealed = 1 THEN g.piece_size ELSE 0 END) AS stored_deal_size,
        COUNT(CASE WHEN d.failed = 0 AND d.sealed = 0 THEN 1 ELSE NULL END) AS deals_in_progress,
        COUNT(CASE WHEN d.sealed = 1 THEN 1 ELSE NULL END) AS deals_done,
        COUNT(CASE WHEN d.failed = 1 THEN 1 ELSE NULL END) AS deals_failed
    FROM
        deals d
            JOIN
        groups g ON d.group_id = g.id
    GROUP BY
        d.group_id
)
SELECT
    COALESCE(SUM(deals_in_progress+deals_done), 0) AS total_non_failed_deal_count,
    COALESCE(SUM(total_data_size), 0) AS total_data_size,
    COALESCE(SUM(total_deal_size), 0) AS total_deal_size,
    COALESCE(SUM(stored_data_size), 0) AS stored_data_size,
    COALESCE(SUM(stored_deal_size), 0) AS stored_deal_size,
    COALESCE(SUM(deals_in_progress), 0) AS deals_in_progress,
    COALESCE(SUM(deals_done), 0) AS deals_done,
    COALESCE(SUM(deals_failed), 0) AS deals_failed
FROM
    deal_summary
;`)
	if err != nil {
		return iface.DealSummary{}, xerrors.Errorf("finding writable groups: %w", err)
	}
	defer res.Close()

	var ds iface.DealSummary

	if res.Next() {
		err := res.Scan(&ds.NonFailed, &ds.TotalDataSize, &ds.TotalDealSize,
			&ds.StoredDataSize, &ds.StoredDealSize,
			&ds.InProgress, &ds.Done, &ds.Failed)
		if err != nil {
			return iface.DealSummary{}, xerrors.Errorf("scanning group: %w", err)
		}
	}

	return ds, nil
}

func (r *ribsDB) ProviderInfo(providerID int64) (iface.ProviderInfo, error) {
	var pInfo iface.ProviderInfo
	err := r.db.QueryRow(`
		SELECT id, ping_ok, boost_deals, booster_http, booster_bitswap,
		indexed_success, indexed_fail, ask_price, ask_verif_price,
		ask_min_piece_size, ask_max_piece_size
		FROM providers WHERE id = ?`, providerID).Scan(
		&pInfo.Meta.ID, &pInfo.Meta.PingOk, &pInfo.Meta.BoostDeals,
		&pInfo.Meta.BoosterHttp, &pInfo.Meta.BoosterBitswap, &pInfo.Meta.IndexedSuccess,
		&pInfo.Meta.IndexedFail,
		&pInfo.Meta.AskPrice, &pInfo.Meta.AskVerifiedPrice, &pInfo.Meta.AskMinPieceSize, &pInfo.Meta.AskMaxPieceSize)
	if err != nil {
		return pInfo, xerrors.Errorf("querying provider metadata: %w", err)
	}

	res, err := r.db.Query("select uuid, provider_addr, sealed, failed, rejected, deal_id, sp_status, sp_sealing_status, error_msg, sp_recv_bytes, sp_txsize, sp_pub_msg_cid, start_epoch, end_epoch, start_time from deals where provider_addr = ? ORDER BY start_time DESC LIMIT 100", providerID)
	if err != nil {
		return pInfo, xerrors.Errorf("getting group meta: %w", err)
	}
	defer res.Close()

	for res.Next() {
		var dealUuid string
		var provider int64
		var sealed, failed, rejected bool
		var startEpoch, endEpoch, startTime int64
		var status *string
		var sealStatus *string
		var errMsg *string
		var bytesRecv *int64
		var txSize *int64
		var pubCid *string
		var dealID *int64

		err := res.Scan(&dealUuid, &provider, &sealed, &failed, &rejected, &dealID, &status, &sealStatus, &errMsg, &bytesRecv, &txSize, &pubCid, &startEpoch, &endEpoch, &startTime)
		if err != nil {
			return pInfo, xerrors.Errorf("scanning deal: %w", err)
		}

		pInfo.RecentDeals = append(pInfo.RecentDeals, iface.DealMeta{
			UUID:       dealUuid,
			Provider:   provider,
			Sealed:     sealed,
			Failed:     failed,
			Rejected:   rejected,
			StartEpoch: startEpoch,
			EndEpoch:   endEpoch,
			StartTime:  startTime,
			Status:     DerefOr(status, ""),
			SealStatus: DerefOr(sealStatus, ""),
			Error:      DerefOr(errMsg, ""),
			DealID:     DerefOr(dealID, 0),
			BytesRecv:  DerefOr(bytesRecv, 0),
			TxSize:     DerefOr(txSize, 0),
			PubCid:     DerefOr(pubCid, ""),
		})
	}

	if err := res.Err(); err != nil {
		return iface.ProviderInfo{}, err
	}
	if err := res.Close(); err != nil {
		return iface.ProviderInfo{}, err
	}

	return pInfo, nil
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
	defer res.Close()

	var found bool

	if res.Next() {
		var commp, root []byte
		var pieceSize, carSize int64
		err := res.Scan(&commp, &root, &pieceSize, &carSize)
		if err != nil {
			return dealParams{}, xerrors.Errorf("scanning group: %w", err)
		}

		out.CommP = commp
		_, out.Root, err = cid.CidFromBytes(root)
		if err != nil {
			return dealParams{}, xerrors.Errorf("parsing cid: %w", err)
		}
		out.PieceSize = pieceSize
		out.CarSize = carSize

		found = true
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

	var LibP2PMaddrsJson string
	var BitswapMaddrsJson string
	var HttpMaddrsJson string

	if len(pres.LibP2PMaddrs) > 0 {
		a, err := json.Marshal(pres.LibP2PMaddrs)
		if err != nil {
			return xerrors.Errorf("marshal libp2p maddrs: %w", err)
		}
		LibP2PMaddrsJson = string(a)
	}
	if len(pres.BitswapMaddrs) > 0 {
		a, err := json.Marshal(pres.BitswapMaddrs)
		if err != nil {
			return xerrors.Errorf("marshal bitswap maddrs: %w", err)
		}
		BitswapMaddrsJson = string(a)
	}
	if len(pres.HttpMaddrs) > 0 {
		a, err := json.Marshal(pres.HttpMaddrs)
		if err != nil {
			return xerrors.Errorf("marshal http maddrs: %w", err)
		}
		HttpMaddrsJson = string(a)
	}

	_, err := r.db.Exec(`
	update providers set ping_ok = ?, boost_deals = ?, booster_http = ?, booster_bitswap = ?, addr_info_graphsync = ?, addr_info_bitswap = ?, addr_info_http = ? where id = ?;
	`, pres.PingOk, pres.BoostDeals, pres.BoosterHttp, pres.BoosterBitswap, LibP2PMaddrsJson, BitswapMaddrsJson, HttpMaddrsJson,
		provider)
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

func (r *ribsDB) GroupDeals(gk iface.GroupKey) ([]iface.DealMeta, error) {
	dealMeta := make([]iface.DealMeta, 0)

	res, err := r.db.Query(`select uuid, provider_addr, sealed, failed, rejected, deal_id,
										sp_status, sp_sealing_status, error_msg, sp_recv_bytes, sp_txsize, sp_pub_msg_cid, start_epoch, end_epoch,
										retrieval_probes_success, retrieval_probes_fail, retrieval_probe_prev_ttfb_ms,
										last_retrieval_check > 0 AND last_retrieval_check > (last_retrieval_check_success + 3600*24) as no_recent_retr
										from deals where group_id = ?`, gk)
	if err != nil {
		return nil, xerrors.Errorf("getting group meta: %w", err)
	}
	defer res.Close()

	for res.Next() {
		var dealUuid string
		var provider int64
		var sealed, failed, rejected bool
		var startEpoch, endEpoch int64
		var status *string
		var sealStatus *string
		var errMsg *string
		var bytesRecv *int64
		var txSize *int64
		var pubCid *string
		var dealID *int64
		var retrievalProbesSuccess *int64
		var retrievalProbesFail *int64
		var retrievalProbeTTFBMS *int64
		var noRecentRetrievalSuccess bool

		err := res.Scan(&dealUuid, &provider, &sealed, &failed, &rejected, &dealID, &status, &sealStatus, &errMsg, &bytesRecv, &txSize, &pubCid, &startEpoch, &endEpoch, &retrievalProbesSuccess, &retrievalProbesFail, &retrievalProbeTTFBMS, &noRecentRetrievalSuccess)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal: %w", err)
		}

		dealMeta = append(dealMeta, iface.DealMeta{
			UUID:       dealUuid,
			Provider:   provider,
			Sealed:     sealed,
			Failed:     failed,
			Rejected:   rejected,
			StartEpoch: startEpoch,
			EndEpoch:   endEpoch,
			Status:     DerefOr(status, ""),
			SealStatus: DerefOr(sealStatus, ""),
			Error:      DerefOr(errMsg, ""),
			DealID:     DerefOr(dealID, 0),
			BytesRecv:  DerefOr(bytesRecv, 0),
			TxSize:     DerefOr(txSize, 0),
			PubCid:     DerefOr(pubCid, ""),

			RetrTTFBMs:  DerefOr(retrievalProbeTTFBMS, 0),
			RetrSuccess: DerefOr(retrievalProbesSuccess, 0),
			RetrFail:    DerefOr(retrievalProbesFail, 0),

			NoRecentSuccess: noRecentRetrievalSuccess,
		})
	}

	sort.SliceStable(dealMeta, func(i, j int) bool {
		return (dealMeta[i].Sealed && !dealMeta[j].Sealed) || (!dealMeta[i].Failed && dealMeta[j].Failed)
	})

	if err := res.Err(); err != nil {
		return nil, xerrors.Errorf("iterating deals: %w", err)
	}

	if err := res.Close(); err != nil {
		return nil, xerrors.Errorf("closing deals iterator: %w", err)
	}

	return dealMeta, nil
}

type noSectorDealInfo struct {
	UUID     string
	Provider int64
	DealID   int64
}

func (r *ribsDB) GetSealedDealsWithNoSectorNums() ([]noSectorDealInfo, error) {
	res, err := r.db.Query(`select uuid, provider_addr, deal_id from deals where deal_id is not null and sector_number is null order by provider_addr limit 1000`)
	if err != nil {
		return nil, xerrors.Errorf("getting deals: %w", err)
	}
	defer res.Close()

	var out []noSectorDealInfo
	for res.Next() {
		var d noSectorDealInfo
		if err := res.Scan(&d.UUID, &d.Provider, &d.DealID); err != nil {
			return nil, xerrors.Errorf("scanning deal: %w", err)
		}
		out = append(out, d)
	}

	return out, nil
}

func (r *ribsDB) FillDealSectorNumber(uuid string, sectorNum abi.SectorNumber) error {
	_, err := r.db.Exec(`update deals set sector_number = ? where uuid = ?`, sectorNum, uuid)
	if err != nil {
		return xerrors.Errorf("updating deal: %w", err)
	}

	return nil
}

type GroupDealStats struct {
	GroupID        int64
	State          iface.GroupState
	TotalDeals     int64
	PublishedDeals int64
	SealedDeals    int64
	FailedDeals    int64
	RejectedDeals  int64
	Retrievable    int64
	Unretrievable  int64
}

func (r *ribsDB) GetGroupDealStats() (map[int64]GroupDealStats, error) {
	query := `
        SELECT
    g.id AS group_id,
    g.g_state AS group_state,
    COUNT(d.group_id) AS total_deals,
    COUNT(CASE WHEN d.published = 1 THEN 1 ELSE NULL END) AS published_deals,
    COUNT(CASE WHEN d.sealed = 1 THEN 1 ELSE NULL END) AS sealed_deals,
    COUNT(CASE WHEN d.failed = 1 THEN 1 ELSE NULL END) AS failed_deals,
    COUNT(CASE WHEN d.rejected = 1 THEN 1 ELSE NULL END) AS rejected_deals,
    COUNT(CASE WHEN d.failed = 0 AND d.last_retrieval_check > 0 AND d.last_retrieval_check < (d.last_retrieval_check_success + 3600*24) THEN 1 ELSE NULL END) AS retrievable_deals,
    COUNT(CASE WHEN d.failed = 0 AND d.last_retrieval_check > 0 AND d.last_retrieval_check > (d.last_retrieval_check_success + 3600*24) THEN 1 ELSE NULL END) AS unretrievable_deals
FROM
    groups g
        LEFT JOIN
    deals d ON g.id = d.group_id
GROUP BY
    g.id;`

	rows, err := r.db.Query(query)
	if err != nil {
		return nil, xerrors.Errorf("fetch group deal stats: %w", err)
	}
	defer rows.Close()

	stats := make(map[int64]GroupDealStats)
	for rows.Next() {
		var s GroupDealStats
		err := rows.Scan(&s.GroupID, &s.State, &s.TotalDeals, &s.PublishedDeals, &s.SealedDeals, &s.FailedDeals, &s.RejectedDeals, &s.Retrievable, &s.Unretrievable)
		if err != nil {
			return nil, xerrors.Errorf("scan group deal stats: %w", err)
		}
		stats[s.GroupID] = s
	}

	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("iterate group deal stats: %w", err)
	}

	return stats, nil
}

func DerefOr[T any](v *T, def T) T {
	if v == nil {
		return def
	}
	return *v
}

type TransferInfo struct {
	Failed                 int
	CarTransferAttempts    int
	CarTransferStartTime   *int64
	CarTransferLastBytes   *int64
	CarTransferLastEndTime *int64
}

func (r *ribsDB) GetTransferStatusByDealUUID(dealUUID uuid.UUID) (*TransferInfo, error) {
	var transferInfo TransferInfo

	err := r.db.QueryRow(`select failed, car_transfer_attempts, car_transfer_start_time, car_transfer_last_bytes, car_transfer_last_end_time from deals where uuid = ?`, dealUUID).Scan(&transferInfo.Failed, &transferInfo.CarTransferAttempts, &transferInfo.CarTransferStartTime, &transferInfo.CarTransferLastBytes, &transferInfo.CarTransferLastEndTime)
	if err != nil {
		return nil, xerrors.Errorf("getting transfer status: %w", err)
	}

	if transferInfo.CarTransferAttempts == 0 {
		_, err := r.db.Exec(`update deals set car_transfer_start_time = ? where uuid = ?`, time.Now().Unix(), dealUUID)
		if err != nil {
			return nil, xerrors.Errorf("setting transfer start time: %w", err)
		}
	}

	return &transferInfo, nil
}

func (r *ribsDB) UpdateTransferStats(dealUUID uuid.UUID, lastBytes int64, abortError error) error {
	failed := 0
	errorMsg := ""

	if abortError != nil {
		failed = 1
		errorMsg = abortError.Error()
	}

	_, err := r.db.Exec(`update deals set car_transfer_last_end_time = ?, car_transfer_last_bytes = ?, car_transfer_attempts = car_transfer_attempts + 1, failed = ?, error_msg = CASE WHEN error_msg = '' THEN ? ELSE error_msg END where uuid = ?`, time.Now().Unix(), lastBytes, failed, errorMsg, dealUUID)
	if err != nil {
		return xerrors.Errorf("updating transfer stats: %w", err)
	}

	return nil
}

type RetrCheckCandidate struct {
	DealID   string
	Provider int64
	Group    int64
	Verified bool
	FastRetr bool
}

func (r *ribsDB) GetRetrievalCheckCandidates() ([]RetrCheckCandidate, error) {
	const secondsIn6Hours = 6 * 60 * 60
	now := time.Now().Unix()

	rows, err := r.db.Query(`
		SELECT uuid, provider_addr, group_id, verified, keep_unsealed FROM deals 
		WHERE sealed = 1 
		AND failed = 0 
		AND last_retrieval_check <= ?`,
		now-secondsIn6Hours)
	if err != nil {
		return nil, xerrors.Errorf("getting retrieval check candidates: %w", err)
	}
	defer rows.Close()

	var deals []RetrCheckCandidate
	for rows.Next() {
		var deal RetrCheckCandidate
		// Assuming Deal is a struct that can scan all columns from the deals table
		err := rows.Scan(&deal.DealID, &deal.Provider, &deal.Group, &deal.Verified, &deal.FastRetr)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal: %w", err)
		}
		deals = append(deals, deal)
	}

	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("iterating rows: %w", err)
	}

	return deals, nil
}

type RetrievalResult struct {
	Success bool
	Error   string

	Duration        time.Duration
	TimeToFirstByte time.Duration
}

func (r *ribsDB) RecordRetrievalCheckResult(dealId string, res RetrievalResult) error {
	// Convert durations to milliseconds for storing in the database.
	durationMs := int(res.Duration / time.Millisecond)
	ttfbMs := int(res.TimeToFirstByte / time.Millisecond)

	// Determine success or failure count increment.
	successIncrement := 0
	if res.Success {
		successIncrement = 1
	}

	// Prepare error message string.
	var errMsg *string
	if res.Error != "" {
		errMsg = &res.Error
	}

	_, err := r.db.Exec(`
        UPDATE deals SET
            last_retrieval_check = strftime('%s', 'now'),
            last_retrieval_check_success = CASE WHEN ? THEN strftime('%s', 'now') ELSE last_retrieval_check_success END,
            retrieval_probe_prev_ms = ?,
            retrieval_probe_prev_ttfb_ms = ?,
            retrieval_probes_success = retrieval_probes_success + ?,
            retrieval_probes_fail = retrieval_probes_fail + ?,
            retrieval_probe_prev_error = ?
        WHERE uuid = ?`,
		res.Success, durationMs, ttfbMs, successIncrement, 1-successIncrement, errMsg, dealId)

	if err != nil {
		return xerrors.Errorf("updating retrieval check result: %w", err)
	}

	return nil
}

type RetrCandidate struct {
	DealID                    string
	Provider                  int64
	Verified                  bool
	FastRetr                  bool
	LastRetrievalCheckSuccess int64
}

func (r *ribsDB) GetRetrievalCandidates(group iface.GroupKey) ([]RetrCandidate, error) {
	rows, err := r.db.Query(`
		SELECT uuid, provider_addr, verified, keep_unsealed, last_retrieval_check_success FROM deals 
		WHERE group_id = ? AND sealed = 1 AND failed = 0 order by retrieval_probe_prev_ttfb_ms asc, last_retrieval_check_success desc, keep_unsealed desc`,
		group)
	if err != nil {
		return nil, xerrors.Errorf("getting retrieval candidates: %w", err)
	}
	defer rows.Close()

	var deals []RetrCandidate
	for rows.Next() {
		var deal RetrCandidate
		// Assuming Deal is a struct that can scan all columns from the deals table
		err := rows.Scan(&deal.DealID, &deal.Provider, &deal.Verified, &deal.FastRetr, &deal.LastRetrievalCheckSuccess)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal: %w", err)
		}
		deals = append(deals, deal)
	}

	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("iterating rows: %w", err)
	}

	return deals, nil
}

type ProviderAddrInfo struct {
	LibP2PMaddrs  []multiaddr.Multiaddr
	BitswapMaddrs []multiaddr.Multiaddr
	HttpMaddrs    []multiaddr.Multiaddr
}

func (r *ribsDB) GetProviderAddrs(provider int64) (ProviderAddrInfo, error) {
	/*
	   addr_info_graphsync text, // json of []multiaddr.Multiaddr
	   addr_info_bitswap text,
	   addr_info_http text
	*/

	var addrInfo ProviderAddrInfo
	var addrInfoGraphsync string
	var addrInfoBitswap string
	var addrInfoHttp string

	err := r.db.QueryRow(`
		SELECT addr_info_graphsync, addr_info_bitswap, addr_info_http FROM providers
		WHERE id = ?`, provider).Scan(&addrInfoGraphsync, &addrInfoBitswap, &addrInfoHttp)
	if err != nil {
		return addrInfo, xerrors.Errorf("query: %w", err)
	}

	if addrInfoGraphsync != "" {
		var strings []string
		err = json.Unmarshal([]byte(addrInfoGraphsync), &strings)
		if err != nil {
			return addrInfo, xerrors.Errorf("unmarshal graphsync: %w", err)
		}

		for _, s := range strings {
			maddr, err := multiaddr.NewMultiaddr(s)
			if err != nil {
				return addrInfo, xerrors.Errorf("parsing graphsync multiaddr: %w", err)
			}
			addrInfo.LibP2PMaddrs = append(addrInfo.LibP2PMaddrs, maddr)
		}
	}

	if addrInfoBitswap != "" {
		var strings []string
		err = json.Unmarshal([]byte(addrInfoBitswap), &strings)
		if err != nil {
			return addrInfo, xerrors.Errorf("unmarshal bitswap: %w", err)
		}

		for _, s := range strings {
			maddr, err := multiaddr.NewMultiaddr(s)
			if err != nil {
				return addrInfo, xerrors.Errorf("parsing bitswap multiaddr: %w", err)
			}
			addrInfo.BitswapMaddrs = append(addrInfo.BitswapMaddrs, maddr)
		}
	}

	if addrInfoHttp != "" {
		var strings []string
		err = json.Unmarshal([]byte(addrInfoHttp), &strings)
		if err != nil {
			return addrInfo, xerrors.Errorf("unmarshal http: %w", err)
		}

		for _, s := range strings {
			maddr, err := multiaddr.NewMultiaddr(s)
			if err != nil {
				return addrInfo, xerrors.Errorf("parsing http multiaddr: %w", err)
			}
			addrInfo.HttpMaddrs = append(addrInfo.HttpMaddrs, maddr)
		}
	}

	return addrInfo, nil
}

func (r *ribsDB) HasS3Offload(group iface.GroupKey) (bool, error) {
	var has int
	err := r.db.QueryRow(`select count(*) from offloads_s3 where group_id = ?`, group).Scan(&has)
	if err != nil {
		return false, xerrors.Errorf("query: %w", err)
	}

	return has > 0, nil
}

func (r *ribsDB) NeedS3Offload() (bool, error) {
	var has int
	err := r.db.QueryRow(`select count(*) from offloads_s3`).Scan(&has)
	if err != nil {
		return false, xerrors.Errorf("query: %w", err)
	}

	return has > 0, nil
}

func (r *ribsDB) AddS3Offload(group iface.GroupKey) error {
	_, err := r.db.Exec(`insert into offloads_s3 (group_id) values (?)`, group)
	if err != nil {
		return xerrors.Errorf("exec: %w", err)
	}

	return nil
}

func (r *ribsDB) DropS3Offload(group iface.GroupKey) error {
	_, err := r.db.Exec(`delete from offloads_s3 where group_id = ?`, group)
	if err != nil {
		return xerrors.Errorf("exec: %w", err)
	}

	return nil
}

func (r *ribsDB) LastTotalUploadedBytes() (int64, error) {
	var b *int64
	err := r.db.QueryRow(`select sum(sp_recv_bytes) from deals`).Scan(&b)
	if err != nil {
		return 0, xerrors.Errorf("querying last transferred bytes: %w", err)
	}

	if b == nil {
		return 0, nil
	}

	return *b, nil
}

func (r *ribsDB) GetRetrievableDealStats() ([]iface.DealCountStats, error) {
	query := `SELECT
    COALESCE(retrievable_count, 0) AS "X retrievable deals",
    COUNT(*) AS "Number of groups"
FROM
    (
        SELECT
            all_groups.group_id,
            COUNT(d.group_id) AS retrievable_count
        FROM
            (SELECT DISTINCT group_id FROM deals) AS all_groups
        LEFT JOIN
            deals d ON all_groups.group_id = d.group_id AND d.last_retrieval_check > 0 AND d.last_retrieval_check < (d.last_retrieval_check_success + 3600*24)
        JOIN groups g on g.id = all_groups.group_id
        WHERE
            g.g_state in (4, 5)
        GROUP BY
            all_groups.group_id
    ) AS retrievable_deals_per_group
GROUP BY
    retrievable_count
ORDER BY
    retrievable_count;
`

	rows, err := r.db.Query(query)
	if err != nil {
		return nil, xerrors.Errorf("getting retrievable deal stats: %w", err)
	}
	defer rows.Close()

	var stats []iface.DealCountStats
	for rows.Next() {
		var stat iface.DealCountStats
		err := rows.Scan(&stat.Count, &stat.Groups)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal stats: %w", err)
		}
		stats = append(stats, stat)
	}

	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("iterating rows: %w", err)
	}

	return stats, nil
}

func (r *ribsDB) GetSealedDealStats() ([]iface.DealCountStats, error) {
	query := `SELECT
    COALESCE(retrievable_count, 0) AS "X sealed deals",
    COUNT(*) AS "Number of groups"
FROM
    (
        SELECT
            all_groups.group_id,
            COUNT(d.group_id) AS retrievable_count
        FROM
            (SELECT DISTINCT group_id FROM deals) AS all_groups
        LEFT JOIN
            deals d ON all_groups.group_id = d.group_id AND d.sealed = 1
        JOIN groups g on g.id = all_groups.group_id
        WHERE
            g.g_state in (4, 5)
        GROUP BY
            all_groups.group_id
    ) AS retrievable_deals_per_group
GROUP BY
    retrievable_count
ORDER BY
    retrievable_count;`

	rows, err := r.db.Query(query)
	if err != nil {
		return nil, xerrors.Errorf("getting sealed deal stats: %w", err)
	}
	defer rows.Close()

	var stats []iface.DealCountStats
	for rows.Next() {
		var stat iface.DealCountStats
		err := rows.Scan(&stat.Count, &stat.Groups)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal stats: %w", err)
		}
		stats = append(stats, stat)
	}

	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("iterating rows: %w", err)
	}

	return stats, nil
}

func (r *ribsDB) AddRepairsForLowRetrievableDeals() error {
	query := `
        INSERT INTO repairs (group_id, retrievable_deals)
			SELECT
				all_groups.group_id,
				COALESCE(COUNT(d.group_id), 0)
			FROM
				(SELECT DISTINCT d.group_id FROM deals d JOIN groups g ON d.group_id = g.id WHERE g.g_state = 4) AS all_groups
			LEFT JOIN
				deals d ON all_groups.group_id = d.group_id AND d.last_retrieval_check > 0 AND d.last_retrieval_check < (d.last_retrieval_check_success + 3600*24)
			GROUP BY
				all_groups.group_id
			HAVING
				COALESCE(COUNT(d.group_id), 0) < 3
		ON CONFLICT (group_id) DO UPDATE
		SET retrievable_deals = EXCLUDED.retrievable_deals;
    `
	_, err := r.db.Exec(query)
	return err
}

func (r *ribsDB) AssignRepairToWorker(workerID int) (*iface.GroupKey, error) {
	query := `
        UPDATE repairs
        SET worker = ?
        WHERE group_id = (
            SELECT group_id FROM repairs
            WHERE worker IS NULL
            ORDER BY last_attempt ASC, retrievable_deals ASC
            LIMIT 1
        )
        RETURNING group_id;
    `
	var groupID iface.GroupKey
	err := r.db.QueryRow(query, workerID).Scan(&groupID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// No rows were updated, return nil
			return nil, nil
		}
		// Other errors
		return nil, err
	}
	return &groupID, nil
}

func (r *ribsDB) GetRepairStats() (out iface.RepairQueueStats, err error) {
	query := `
        SELECT COUNT(*), COUNT(CASE WHEN worker IS NOT NULL THEN 1 END)
        FROM repairs;
    `
	err = r.db.QueryRow(query).Scan(&out.Total, &out.Assigned)
	return out, err
}

func (r *ribsDB) GetAssignedRepairWorkByWorkerID(workerID int) ([]iface.GroupKey, error) {
	query := `
        SELECT group_id FROM repairs
        WHERE worker = ?;
    `

	rows, err := r.db.Query(query, workerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var groupIDs []iface.GroupKey
	for rows.Next() {
		var groupID iface.GroupKey
		if err := rows.Scan(&groupID); err != nil {
			return nil, err
		}
		groupIDs = append(groupIDs, groupID)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return groupIDs, nil
}

func (r *ribsDB) DelRepair(groupID iface.GroupKey) error {
	query := `
		DELETE FROM repairs
		WHERE group_id = ?;
	`
	_, err := r.db.Exec(query, groupID)
	return err
}

func (r *ribsDB) UpdateRepairOnStepNotDone(workerID int) error {
	query := `
		UPDATE repairs
		SET
		    worker = NULL,
		    last_attempt = strftime('%s','now')
		WHERE worker = ?;
	`

	_, err := r.db.Exec(query, workerID)
	if err != nil {
		return fmt.Errorf("failed to update repair: %w", err)
	}

	return nil
}
