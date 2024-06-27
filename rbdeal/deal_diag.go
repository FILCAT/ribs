package rbdeal

import (
	"context"

	"github.com/libp2p/go-libp2p/core/host"

	iface "github.com/atboosty/ribs"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
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

func (r *ribs) GroupDeals(gk iface.GroupKey) ([]iface.DealMeta, error) {
	return r.db.GroupDeals(gk)
}

func (r *ribs) StagingStats() (iface.StagingStats, error) {
	return iface.StagingStats{
		UploadBytes:   r.s3UploadBytes.Load(),
		UploadStarted: r.s3UploadStarted.Load(),
		UploadDone:    r.s3UploadDone.Load(),
		UploadErr:     r.s3UploadErr.Load(),
		Redirects:     r.s3Redirects.Load(),
		ReadReqs:      r.s3ReadReqs.Load(),
		ReadBytes:     r.s3ReadBytes.Load(),
	}, nil
}

func (r *ribs) Filecoin(ctx context.Context) (api.Gateway, jsonrpc.ClientCloser, error) {
	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		panic(err)
	}

	return gw, closer, nil
}

func getLibP2PInfoForHost(h host.Host) iface.Libp2pInfo {
	out := iface.Libp2pInfo{
		PeerID: h.ID().String(),
		Peers:  len(h.Network().Peers()),
	}

	for _, ma := range h.Network().ListenAddresses() {
		out.Listen = append(out.Listen, ma.String())
	}

	return out
}

func (r *ribs) P2PNodes(ctx context.Context) (map[string]iface.Libp2pInfo, error) {
	out := map[string]iface.Libp2pInfo{}

	out["main"] = getLibP2PInfoForHost(r.host)
	out["crawl"] = getLibP2PInfoForHost(r.crawlHost)
	out["lassie"] = getLibP2PInfoForHost(r.retrHost)

	return out, nil
}

func (r *ribs) RetrChecker() iface.RetrCheckerStats {
	return iface.RetrCheckerStats{
		ToDo:       r.rckToDo.Load(),
		Started:    r.rckStarted.Load(),
		Success:    r.rckSuccess.Load(),
		Fail:       r.rckFail.Load(),
		SuccessAll: r.rckSuccessAll.Load(),
		FailAll:    r.rckFailAll.Load(),
	}
}

func (r *ribs) RetrievableDealCounts() ([]iface.DealCountStats, error) {
	return r.db.GetRetrievableDealStats()
}

func (r *ribs) SealedDealCounts() ([]iface.DealCountStats, error) {
	return r.db.GetSealedDealStats()
}

func (r *ribs) RepairQueue() (iface.RepairQueueStats, error) {
	return r.db.GetRepairStats()
}

func (r *ribs) RepairStats() (map[int]iface.RepairJob, error) {
	r.repairStatsLk.Lock()
	defer r.repairStatsLk.Unlock()

	out := map[int]iface.RepairJob{}
	for k, v := range r.repairStats {
		out[k] = *v
	}

	return out, nil
}
