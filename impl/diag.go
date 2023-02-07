package impl

import (
	iface "github.com/lotus_web3/ribs"
	"golang.org/x/xerrors"
)

func (r *ribs) Diagnostics() iface.Diag {
	return r
}

func (r *ribs) Groups() ([]iface.GroupKey, error) {
	r.lk.Lock()
	defer r.lk.Unlock()

	res, err := r.db.Query("select id, from groups")
	if err != nil {
		return nil, xerrors.Errorf("finding writable groups: %w", err)
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

func (r *ribs) GroupMeta(gk iface.GroupKey) (iface.GroupMeta, error) {
	r.lk.Lock()
	defer r.lk.Unlock()

	res, err := r.db.Query("select blocks, bytes, g_state from groups where id = ?", gk)
	if err != nil {
		return iface.GroupMeta{}, xerrors.Errorf("finding writable groups: %w", err)
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

		Blocks: blocks,
		Bytes:  bytes,
	}, nil
}
