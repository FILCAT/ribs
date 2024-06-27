package rbstor

import (
	"context"
	"sync/atomic"

	iface "github.com/atboosty/ribs"
	"golang.org/x/xerrors"
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
		m.ReadBlocks = g.readBlocks.Load()
		m.ReadBytes = g.readSize.Load()
		m.WriteBlocks = g.writeBlocks.Load()
		m.WriteBytes = g.writeSize.Load()
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
		readBlocks := group.readBlocks.Load()
		readSize := group.readSize.Load()
		writeBlocks := group.writeBlocks.Load()
		writeSize := group.writeSize.Load()

		r.grpReadBlocks += readBlocks - group.readBlocksSnap
		r.grpReadSize += readSize - group.readSizeSnap
		r.grpWriteBlocks += writeBlocks - group.writeBlocksSnap
		r.grpWriteSize += writeSize - group.writeSizeSnap

		group.readBlocksSnap = readBlocks
		group.readSizeSnap = readSize
		group.writeBlocksSnap = writeBlocks
		group.writeSizeSnap = writeSize
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

func (r *rbs) WorkerStats() iface.WorkerStats {
	return iface.WorkerStats{
		Available:  r.workersAvail.Load(),
		InFinalize: r.workersFinalizing.Load(),
		InCommP:    r.workersCommP.Load(),
		InReload:   r.workersFinDataReload.Load(),
		TaskQueue:  int64(len(r.tasks)),
		CommPBytes: globalCommpBytes.Load(),
	}
}
