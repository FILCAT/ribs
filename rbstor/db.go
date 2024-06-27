package rbstor

import (
	"context"
	"database/sql"
	"path/filepath"

	"github.com/atboosty/ribs/ributil"

	iface "github.com/atboosty/ribs"
	commcid "github.com/filecoin-project/go-fil-commcid"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
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
	"PRAGMA busy_timeout = 50000",
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
     * 2 - vrcar done
     * 3 - has commp
     * 4 - offloaded
     * 5 - reload
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

CREATE VIEW IF NOT EXISTS group_stats_view AS
SELECT 
    COUNT(*) AS group_count,
    SUM(bytes) AS total_data_size,
    SUM(CASE WHEN g_state < 4 THEN bytes ELSE 0 END) AS non_offloaded_data_size,
    SUM(CASE WHEN g_state = 4 THEN bytes ELSE 0 END) AS offloaded_data_size
FROM 
    groups;

create table if not exists offloads
(
	group_id  integer not null
	    constraint offloads_groups_id_pk
	    		    primary key
		constraint offloads_groups_id_fk
			references groups
				on update cascade on delete cascade
);

create index if not exists offloads_group_id_index
	on offloads (group_id);
`

type rbsDB struct {
	db *ributil.RetryDB
}

func openRibsDB(root string, opt *ributil.RetryDB) (*rbsDB, error) {
	db := opt
	if db == nil {
		var err error
		rdb, err := sql.Open("sqlite3", filepath.Join(root, "store.db"))
		if err != nil {
			return nil, xerrors.Errorf("open db: %w", err)
		}
		db = ributil.NewRetryDB(rdb)
	}

	for _, pragma := range pragmas {
		_, err := db.Exec(pragma)
		if err != nil {
			return nil, xerrors.Errorf("exec pragma: %w", err)
		}
	}

	_, err := db.Exec(dbSchema)
	if err != nil {
		return nil, xerrors.Errorf("exec schema: %w", err)
	}

	return &rbsDB{
		db: db,
	}, nil
}

func (r *rbsDB) GetGroupStats() (*iface.GroupStats, error) {
	var gs iface.GroupStats
	err := r.db.QueryRow(`SELECT group_count, total_data_size, non_offloaded_data_size, offloaded_data_size FROM group_stats_view`).Scan(&gs.GroupCount, &gs.TotalDataSize, &gs.NonOffloadedDataSize, &gs.OffloadedDataSize)
	if err != nil {
		return nil, xerrors.Errorf("querying group stats: %w", err)
	}
	return &gs, nil
}

func (r *rbsDB) GetWritableGroup() (selected iface.GroupKey, blocks, bytes, jbhead int64, state iface.GroupState, err error) {
	res, err := r.db.Query("select id, blocks, bytes, jb_recorded_head, g_state from groups where g_state = 0")
	if err != nil {
		return 0, 0, 0, 0, 0, xerrors.Errorf("finding writable groups: %w", err)
	}
	defer res.Close()

	selectedGroup := iface.UndefGroupKey

	if res.Next() {
		err := res.Scan(&selectedGroup, &blocks, &bytes, &jbhead, &state)
		if err != nil {
			return 0, 0, 0, 0, 0, xerrors.Errorf("scanning group: %w", err)
		}
	}

	if err := res.Err(); err != nil {
		return 0, 0, 0, 0, 0, xerrors.Errorf("iterating groups: %w", err)
	}
	if err := res.Close(); err != nil {
		return 0, 0, 0, 0, 0, xerrors.Errorf("closing group iterator: %w", err)
	}

	return selectedGroup, blocks, bytes, jbhead, state, nil
}

func (r *rbsDB) CreateGroup() (out iface.GroupKey, err error) {
	err = r.db.QueryRow("insert into groups (blocks, bytes, g_state, jb_recorded_head) values (0, 0, 0, 0) returning id").Scan(&out)
	if err != nil {
		return iface.UndefGroupKey, xerrors.Errorf("creating group entry: %w", err)
	}

	return
}

func (r *rbsDB) OpenGroup(gid iface.GroupKey) (blocks, bytes, jbhead int64, state iface.GroupState, err error) {
	res, err := r.db.Query("select blocks, bytes, jb_recorded_head, g_state from groups where id = ?", gid)
	if err != nil {
		return 0, 0, 0, 0, xerrors.Errorf("finding writable groups: %w", err)
	}
	defer res.Close()

	var found bool

	if res.Next() {
		err := res.Scan(&blocks, &bytes, &jbhead, &state)
		if err != nil {
			return 0, 0, 0, 0, xerrors.Errorf("scanning group: %w", err)
		}

		found = true
	}

	if err := res.Err(); err != nil {
		return 0, 0, 0, 0, xerrors.Errorf("iterating groups: %w", err)
	}
	if err := res.Close(); err != nil {
		return 0, 0, 0, 0, xerrors.Errorf("closing group iterator: %w", err)
	}
	if !found {
		return 0, 0, 0, 0, xerrors.Errorf("group %d not found", gid)
	}

	return blocks, bytes, jbhead, state, nil
}

func (r *rbsDB) GroupStates() (gs map[iface.GroupKey]iface.GroupState, err error) {
	res, err := r.db.Query("select id, g_state from groups")
	if err != nil {
		return nil, xerrors.Errorf("finding writable groups: %w", err)
	}
	defer res.Close()

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

func (r *rbsDB) SetGroupHead(ctx context.Context, id iface.GroupKey, state iface.GroupState, commBlk, commSz, at int64) error {
	_, err := r.db.ExecContext(ctx, `update groups set blocks = ?, bytes = ?, g_state = ?, jb_recorded_head = ? where id = ?;`, commBlk, commSz, state, at, id)
	if err != nil {
		return xerrors.Errorf("update group head: %w", err)
	}

	return nil
}

func (r *rbsDB) SetGroupState(ctx context.Context, id iface.GroupKey, state iface.GroupState) error {
	_, err := r.db.ExecContext(ctx, `update groups set g_state = ? where id = ?;`, state, id)
	if err != nil {
		return xerrors.Errorf("update group state: %w", err)
	}

	return nil
}

func (r *rbsDB) SetCommP(ctx context.Context, id iface.GroupKey, state iface.GroupState, commp []byte, paddedPieceSize int64, root cid.Cid, carSize int64) error {
	_, err := r.db.ExecContext(ctx, `update groups set commp = ?, piece_size = ?, root = ?, car_size = ?, g_state = ? where id = ?;`,
		commp[:], paddedPieceSize, root.Bytes(), carSize, state, id)
	if err != nil {
		return xerrors.Errorf("update group commp: %w", err)
	}

	return nil
}

/* DIAGNOSTICS */

func (r *rbsDB) Groups() ([]iface.GroupKey, error) {
	res, err := r.db.Query("select id from groups order by id desc")
	if err != nil {
		return nil, xerrors.Errorf("listing groups: %w", err)
	}
	defer res.Close()

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

func (r *rbsDB) GroupMeta(gk iface.GroupKey) (iface.GroupMeta, error) {
	res, err := r.db.Query("select blocks, bytes, g_state, car_size, commp, root from groups where id = ?", gk)
	if err != nil {
		return iface.GroupMeta{}, xerrors.Errorf("getting group meta: %w", err)
	}
	defer res.Close()

	var blocks int64
	var bytes int64
	var state iface.GroupState
	var found bool
	var carSize *int64
	var commp, root []byte

	if res.Next() {
		err := res.Scan(&blocks, &bytes, &state, &carSize, &commp, &root)
		if err != nil {
			return iface.GroupMeta{}, xerrors.Errorf("scanning group: %w", err)
		}

		found = true
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

	var pcid, rcid string
	if len(commp) > 0 {
		c, err := commcid.PieceCommitmentV1ToCID(commp)
		if err != nil {
			return iface.GroupMeta{}, xerrors.Errorf("parsing commp: %w", err)
		}

		pcid = c.String()
	}
	if len(root) > 0 {
		c, err := cid.Cast(root)
		if err != nil {
			return iface.GroupMeta{}, xerrors.Errorf("parsing root: %w", err)
		}

		rcid = c.String()
	}

	return iface.GroupMeta{
		State: state,

		MaxBlocks: maxGroupBlocks,
		MaxBytes:  maxGroupSize,

		Blocks: blocks,
		Bytes:  bytes,

		DealCarSize: carSize,

		PieceCID: pcid,
		RootCID:  rcid,
	}, nil
}

func (r *rbsDB) DescibeGroup(ctx context.Context, group iface.GroupKey) (iface.GroupDesc, error) {
	var out iface.GroupDesc

	res, err := r.db.QueryContext(ctx, "SELECT root, commp, car_size FROM groups WHERE id = ?", group)
	if err != nil {
		return iface.GroupDesc{}, xerrors.Errorf("finding group: %w", err)
	}
	defer res.Close()

	var found bool

	if res.Next() {
		var root, commp []byte
		err := res.Scan(&root, &commp, &out.CarSize)
		if err != nil {
			return iface.GroupDesc{}, xerrors.Errorf("scanning group: %w", err)
		}

		_, out.RootCid, err = cid.CidFromBytes(root)
		if err != nil {
			return iface.GroupDesc{}, xerrors.Errorf("converting root to cid: %w", err)
		}

		out.PieceCid, err = commcid.DataCommitmentV1ToCID(commp)
		if err != nil {
			return iface.GroupDesc{}, xerrors.Errorf("converting commp to cid: %w", err)
		}

		found = true
	}

	if err := res.Err(); err != nil {
		return iface.GroupDesc{}, xerrors.Errorf("iterating groups: %w", err)
	}

	if !found {
		return iface.GroupDesc{}, xerrors.Errorf("group %d not found", group)
	}

	return out, nil
}

func (r *rbsDB) CountNonOffloadedGroups() (count int, err error) {
	err = r.db.QueryRow("SELECT COUNT(*) FROM groups LEFT JOIN offloads ON groups.id = offloads.group_id WHERE offloads.group_id IS NULL").Scan(&count)
	if err != nil {
		return 0, xerrors.Errorf("counting non-offloaded groups: %w", err)
	}
	return
}

func (r *rbsDB) GetOffloadCandidate() (id iface.GroupKey, err error) {
	err = r.db.QueryRow(`
		SELECT id 
		FROM groups 
		LEFT JOIN offloads ON groups.id = offloads.group_id 
		WHERE offloads.group_id IS NULL AND g_state IN (3, 4) 
		ORDER BY 
			CASE WHEN g_state = 4 THEN 0 ELSE 1 END, 
			id 
		LIMIT 1
	`).Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			return iface.UndefGroupKey, nil
		}
		return 0, xerrors.Errorf("getting priority non-offloaded group ID: %w", err)
	}
	return
}

func (r *rbsDB) WriteOffloadEntry(gid iface.GroupKey) (err error) {
	_, err = r.db.Exec("INSERT OR IGNORE INTO offloads (group_id) VALUES (?)", gid)
	if err != nil {
		return xerrors.Errorf("writing offload entry: %w", err)
	}
	return nil
}
