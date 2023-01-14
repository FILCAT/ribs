package impl

import (
	"context"
	"database/sql"
	iface "github.com/magik6k/carsplit/ribs"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

type Index struct {
	db *sql.DB
}

func NewIndex(db *sql.DB) *Index {
	return &Index{db: db}
}

func (i *Index) GetGroups(ctx context.Context, mh []multihash.Multihash, cb func([][]iface.GroupKey) (more bool, err error)) error {
	stmt, err := i.db.Prepare(`select "group" from "index" where hash = ?`) // todo limit?
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

	stmt, err := tx.PrepareContext(ctx, `INSERT INTO "index" (hash, "group") VALUES (?, ?)`)
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
	//TODO implement me
	panic("implement me")
}

var _ iface.Index = (*Index)(nil)
