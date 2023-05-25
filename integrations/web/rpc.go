package web

import (
	"context"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"

	"github.com/lotus-web3/ribs"
)

type RIBSRpc struct {
	ribs ribs.RIBS
}

func (rc *RIBSRpc) WalletInfo(ctx context.Context) (ribs.WalletInfo, error) {
	return rc.ribs.Wallet().WalletInfo()
}

func (rc *RIBSRpc) WalletMarketAdd(ctx context.Context, amt abi.TokenAmount) (cid.Cid, error) {
	return rc.ribs.Wallet().MarketAdd(ctx, amt)
}

func (rc *RIBSRpc) WalletMarketWithdraw(ctx context.Context, amt abi.TokenAmount) (cid.Cid, error) {
	return rc.ribs.Wallet().MarketWithdraw(ctx, amt)
}

func (rc *RIBSRpc) WalletWithdraw(ctx context.Context, amt abi.TokenAmount, to address.Address) (cid.Cid, error) {
	return rc.ribs.Wallet().Withdraw(ctx, amt, to)
}

func (rc *RIBSRpc) Groups(ctx context.Context) ([]ribs.GroupKey, error) {
	return rc.ribs.StorageDiag().Groups()
}

func (rc *RIBSRpc) GroupMeta(ctx context.Context, group ribs.GroupKey) (ribs.GroupMeta, error) {
	return rc.ribs.StorageDiag().GroupMeta(group)
}

func (rc *RIBSRpc) GroupDeals(ctx context.Context, group ribs.GroupKey) ([]ribs.DealMeta, error) {
	return rc.ribs.DealDiag().GroupDeals(group)
}

func (rc *RIBSRpc) CrawlState(ctx context.Context) (ribs.CrawlState, error) {
	return rc.ribs.DealDiag().CrawlState(), nil
}

func (rc *RIBSRpc) CarUploadStats(ctx context.Context) (ribs.UploadStats, error) {
	return rc.ribs.DealDiag().CarUploadStats(), nil
}

func (rc *RIBSRpc) ReachableProviders(ctx context.Context) ([]ribs.ProviderMeta, error) {
	return rc.ribs.DealDiag().ReachableProviders(), nil
}

func (rc *RIBSRpc) ProviderInfo(ctx context.Context, id int64) (ribs.ProviderInfo, error) {
	return rc.ribs.DealDiag().ProviderInfo(id)
}

func (rc *RIBSRpc) DealSummary(ctx context.Context) (ribs.DealSummary, error) {
	return rc.ribs.DealDiag().DealSummary()
}

func (rc *RIBSRpc) TopIndexStats(ctx context.Context) (ribs.TopIndexStats, error) {
	return rc.ribs.StorageDiag().TopIndexStats(ctx)
}

func (rc *RIBSRpc) GroupIOStats(ctx context.Context) (ribs.GroupIOStats, error) {
	return rc.ribs.StorageDiag().GroupIOStats(), nil
}

func (rc *RIBSRpc) GetGroupStats(ctx context.Context) (*ribs.GroupStats, error) {
	return rc.ribs.StorageDiag().GetGroupStats()
}

func MakeRPCServer(ctx context.Context, ribs ribs.RIBS) (*jsonrpc.RPCServer, jsonrpc.ClientCloser, error) {
	hnd := &RIBSRpc{ribs: ribs}

	fgw, closer, err := ribs.DealDiag().Filecoin(ctx)
	if err != nil {
		return nil, nil, err
	}

	sv := jsonrpc.NewServer()
	sv.Register("RIBS", hnd)
	sv.Register("Filecoin", fgw)

	return sv, closer, nil
}
