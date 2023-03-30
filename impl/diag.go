package impl

import (
	"context"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/types"
	iface "github.com/lotus-web3/ribs"
	"golang.org/x/xerrors"
	"sync/atomic"
	"time"
)

func (r *ribs) Diagnostics() iface.Diag {
	return r
}

func (r *ribs) Groups() ([]iface.GroupKey, error) {
	return r.db.Groups()
}

func (r *ribs) GroupMeta(gk iface.GroupKey) (iface.GroupMeta, error) {
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

func (r *ribs) GetGroupStats() (*iface.GroupStats, error) {
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

func (r *ribs) GroupIOStats() iface.GroupIOStats {
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

func (r *ribs) CrawlState() iface.CrawlState {
	return *r.crawlState.Load()
}

func (r *ribs) ReachableProviders() []iface.ProviderMeta {
	return r.db.ReachableProviders()
}

func (r *ribs) WalletInfo() (iface.WalletInfo, error) {
	r.diagLk.Lock()
	defer r.diagLk.Unlock()

	if r.cachedWalletInfo != nil && time.Since(r.lastWalletInfoUpdate) < time.Minute {
		return *r.cachedWalletInfo, nil
	}

	addr, err := r.wallet.GetDefault()
	if err != nil {
		return iface.WalletInfo{}, xerrors.Errorf("get default wallet: %w", err)
	}

	ctx := context.TODO()

	gw, closer, err := client.NewGatewayRPCV1(ctx, "http://api.chain.love/rpc/v1", nil)
	if err != nil {
		panic(err)
	}
	defer closer()

	b, err := gw.WalletBalance(ctx, addr)
	if err != nil {
		return iface.WalletInfo{}, xerrors.Errorf("get wallet balance: %w", err)
	}

	mb, err := gw.StateMarketBalance(ctx, addr, types.EmptyTSK)
	if err != nil {
		return iface.WalletInfo{}, xerrors.Errorf("get market balance: %w", err)
	}

	wi := iface.WalletInfo{
		Addr:          addr.String(),
		Balance:       types.FIL(b).Short(),
		MarketBalance: types.FIL(mb.Escrow).Short(),
		MarketLocked:  types.FIL(mb.Locked).Short(),
	}

	r.cachedWalletInfo = &wi
	r.lastWalletInfoUpdate = time.Now()

	return wi, nil
}

func (r *ribs) DealSummary() (iface.DealSummary, error) {
	return r.db.DealSummary()
}

func (r *ribs) TopIndexStats(ctx context.Context) (iface.TopIndexStats, error) {
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

func (r *ribs) Filecoin(ctx context.Context) (api.Gateway, jsonrpc.ClientCloser, error) {
	gw, closer, err := client.NewGatewayRPCV1(ctx, "http://api.chain.love/rpc/v1", nil)
	if err != nil {
		panic(err)
	}

	return gw, closer, nil
}
