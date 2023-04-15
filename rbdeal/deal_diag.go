package rbdeal

import (
	"context"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	iface "github.com/lotus-web3/ribs"
)

func (r *ribs) DealDiag() iface.RIBSDiag {
	return r
}

func (r *ribs) CrawlState() iface.CrawlState {
	return *r.crawlState.Load()
}

func (r *ribs) ReachableProviders() []iface.ProviderMeta {
	return r.db.ReachableProviders()
}

func (r *ribs) ProviderInfo(id int64) (iface.ProviderInfo, error) {
	return r.db.ProviderInfo(id)
}

func (r *ribs) DealSummary() (iface.DealSummary, error) {
	return r.db.DealSummary()
}

func (r *ribs) Filecoin(ctx context.Context) (api.Gateway, jsonrpc.ClientCloser, error) {
	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		panic(err)
	}

	return gw, closer, nil
}
