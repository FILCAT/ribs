package impl

import (
	"context"
	"database/sql"
	iface "github.com/lotus-web3/ribs"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

type Index struct {
	db *sql.DB
}

func (i *Index) Close() error {
	return nil // no-op for sqlite
}

func (i *Index) Sync(ctx context.Context) error {
	return nil // no-op for sqlite
}

func NewIndex(db *sql.DB) *Index {
	return &Index{db: db}
}

func (i *Index) GetGroups(ctx context.Context, mh []multihash.Multihash, cb func([][]iface.GroupKey) (more bool, err error)) error {
	stmt, err := i.db.Prepare(`select group_id from top_index where hash = ?`) // todo limit?
	if err != nil {
		return xerrors.Errorf("prepare query: %w", err)
	}

	// todo: parallelize?

	var groups []iface.GroupKey
	for _, m := range mh {
		r, err := stmt.QueryContext(ctx, m)
		if err != nil {
			return xerrors.Errorf("query: %w", err)
		}

		groups = groups[:0]
		for r.Next() {
			var g iface.GroupKey
			if err := r.Scan(&g); err != nil {
				return xerrors.Errorf("scan: %w", err)
			}

			groups = append(groups, g)
		}

		more, err := cb([][]iface.GroupKey{groups})
		if err != nil {
			return xerrors.Errorf("callback: %w", err)
		}

		if !more {
			return nil
		}
	}

	return nil
}

func (i *Index) AddGroup(ctx context.Context, mh []multihash.Multihash, group iface.GroupKey) error {
	tx, err := i.db.BeginTx(ctx, nil)
	if err != nil {
		return xerrors.Errorf("begin tx: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx, `INSERT INTO top_index (hash, group_id) VALUES (?, ?)`)
	if err != nil {
		return xerrors.Errorf("prepare insert: %w", err)
	}

	for _, m := range mh {
		_, err := stmt.ExecContext(ctx, m, group)
		if err != nil {
			return xerrors.Errorf("insert: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return xerrors.Errorf("commit: %w", err)
	}

	return nil
}

func (i *Index) DropGroup(ctx context.Context, mh []multihash.Multihash, group iface.GroupKey) error {
	tx, err := i.db.BeginTx(ctx, nil)
	if err != nil {
		return xerrors.Errorf("begin tx: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx, `DELETE FROM top_index WHERE hash = ? AND group_id = ?`)
	if err != nil {
		return xerrors.Errorf("prepare delete: %w", err)
	}

	for _, m := range mh {
		_, err := stmt.ExecContext(ctx, m, group)
		if err != nil {
			return xerrors.Errorf("delete: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return xerrors.Errorf("commit: %w", err)
	}

	return nil
}

func (i *Index) EstimateSize(ctx context.Context) (int64, error) {
	var count int64
	if err := i.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM top_index`).Scan(&count); err != nil {
		return 0, xerrors.Errorf("query: %w", err)
	}

	return count, nil
}

var _ iface.Index = (*Index)(nil)
