package impl

import (
	"context"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/types"
	iface "github.com/lotus-web3/ribs"
	"golang.org/x/xerrors"
	"sync/atomic"
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
	}

	return m, nil
}

func (r *ribs) CrawlState() string {
	return *r.crawlState.Load()
}

func (r *ribs) ReachableProviders() []iface.ProviderMeta {
	return r.db.ReachableProviders()
}

func (r *ribs) WalletInfo() (iface.WalletInfo, error) {
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

	return iface.WalletInfo{
		Addr:          addr.String(),
		Balance:       types.FIL(b).Short(),
		MarketBalance: types.FIL(mb.Escrow).Short(),
		MarketLocked:  types.FIL(mb.Locked).Short(),
	}, nil
}
