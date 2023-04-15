package rbstor

import (
	"context"
	iface "github.com/lotus-web3/ribs"
	"golang.org/x/xerrors"
	"sync/atomic"
)

func (r *rbs) StorageDiag() iface.RBSDiag {
	return r
}

func (r *rbs) Groups() ([]iface.GroupKey, error) {
	return r.db.Groups()
}

func (r *rbs) GroupMeta(gk iface.GroupKey) (iface.GroupMeta, error) {
	m, err := r.db.GroupMeta(gk)
	if err != nil {
		return iface.GroupMeta{}, xerrors.Errorf("get group meta: %w", err)
	}

	r.lk.Lock()
	g, ok := r.openGroups[gk]
	r.lk.Unlock()

	if ok {
		m.ReadBlocks = atomic.LoadInt64(&g.readBlocks)
		m.ReadBytes = atomic.LoadInt64(&g.readSize)
		m.WriteBlocks = atomic.LoadInt64(&g.writeBlocks)
		m.WriteBytes = atomic.LoadInt64(&g.writeSize)
	}

	return m, nil
}

func (r *rbs) GetGroupStats() (*iface.GroupStats, error) {
	gs, err := r.db.GetGroupStats()
	if err != nil {
		return nil, err
	}

	r.lk.Lock()
	gs.OpenGroups = len(r.openGroups)
	gs.OpenWritable = len(r.writableGroups)
	r.lk.Unlock()

	return gs, nil
}

func (r *rbs) GroupIOStats() iface.GroupIOStats {
	r.lk.Lock()
	defer r.lk.Unlock()

	// first update global counters
	for _, group := range r.openGroups {
		r.grpReadBlocks += group.readBlocks - group.readBlocksSnap
		r.grpReadSize += group.readSize - group.readSizeSnap
		r.grpWriteBlocks += group.writeBlocks - group.writeBlocksSnap
		r.grpWriteSize += group.writeSize - group.writeSizeSnap

		group.readBlocksSnap = group.readBlocks
		group.readSizeSnap = group.readSize
		group.writeBlocksSnap = group.writeBlocks
		group.writeSizeSnap = group.writeSize
	}

	// then return the global counters
	stats := iface.GroupIOStats{
		ReadBlocks:  r.grpReadBlocks,
		ReadBytes:   r.grpReadSize,
		WriteBlocks: r.grpWriteBlocks,
		WriteBytes:  r.grpWriteSize,
	}

	return stats
}

func (r *rbs) TopIndexStats(ctx context.Context) (iface.TopIndexStats, error) {
	s, err := r.index.EstimateSize(ctx)
	if err != nil {
		return iface.TopIndexStats{}, xerrors.Errorf("estimate size: %w", err)
	}

	return iface.TopIndexStats{
		Entries: s,
		Writes:  atomic.LoadInt64(&r.index.writes),
		Reads:   atomic.LoadInt64(&r.index.reads),
	}, nil
}
