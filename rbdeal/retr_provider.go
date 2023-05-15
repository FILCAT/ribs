package rbdeal

import (
	"context"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/lib/must"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	iface "github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/ributil"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
	"sort"
	"sync"
	"time"
)

type mhStr string // multihash bytes in a string

type retrievalProvider struct {
	r *ribs

	lsi *lassie.Lassie

	reqSourcesLk sync.Mutex
	requests     map[mhStr]map[iface.GroupKey]int

	gw api.Gateway

	statLk   sync.Mutex
	attempts map[peer.ID]int64
	fails    map[peer.ID]int64
}

func (r *retrievalProvider) FindCandidates(ctx context.Context, cid cid.Cid) ([]types.RetrievalCandidate, error) {
	return nil, xerrors.Errorf("is this used?")
}

func (r *retrievalProvider) FindCandidatesAsync(ctx context.Context, cid cid.Cid, f func(types.RetrievalCandidate)) error {
	var source iface.GroupKey

	r.reqSourcesLk.Lock()
	if _, ok := r.requests[mhStr(cid.Hash())]; !ok {
		r.reqSourcesLk.Unlock()
		return xerrors.Errorf("no requests for cid")
	}

	for s := range r.requests[mhStr(cid.Hash())] {
		source = s
		break
	}
	r.reqSourcesLk.Unlock()

	candidates, err := r.r.db.GetRetrievalCandidates(source)
	if err != nil {
		return xerrors.Errorf("failed to get retrieval candidates: %w", err)
	}

	gm, err := r.r.Storage().DescibeGroup(ctx, source) // todo cache
	if err != nil {
		return xerrors.Errorf("failed to get group metadata: %w", err)
	}

	log.Infow("got retrieval candidates", "cid", cid, "candidates", len(candidates))

	cs := make([]types.RetrievalCandidate, 0, len(candidates))

	for _, candidate := range candidates {
		maddr, err := address.NewIDAddress(uint64(candidate.Provider))
		if err != nil {
			log.Errorw("failed to parse miner address", "miner", candidate.Provider, "err", err)
			continue
		}

		addrInfo, err := GetAddrInfo(ctx, r.gw, maddr) // todo cache, pull from db
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			continue
		}

		cs = append(cs, types.RetrievalCandidate{
			MinerPeer: *addrInfo,
			RootCid:   gm.RootCid,
			Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{ // todo bitswap (/http?)
				PieceCID:      gm.PieceCid,
				VerifiedDeal:  candidate.Verified,
				FastRetrieval: candidate.FastRetr,
			}),
		})
	}

	r.statLk.Lock()
	sort.SliceStable(cs, func(i, j int) bool {
		if cs[i].MinerPeer.ID == cs[j].MinerPeer.ID {
			return true
		}

		iattempts := r.attempts[cs[i].MinerPeer.ID]
		jattempts := r.attempts[cs[j].MinerPeer.ID]

		ifails := r.fails[cs[i].MinerPeer.ID]
		jfails := r.fails[cs[j].MinerPeer.ID]

		ifailRatio := float64(ifails) / float64(iattempts+1)
		jfailRatio := float64(jfails) / float64(jattempts+1)

		return ifailRatio < jfailRatio // prefer peers that have failed less
	})
	r.statLk.Unlock()
	for _, c := range cs[:3] { // only return the top 3
		r.statLk.Lock()
		log.Errorw("secect", "p", c.MinerPeer.ID, "attempts", r.attempts[c.MinerPeer.ID], "fails", r.fails[c.MinerPeer.ID])
		r.statLk.Unlock()

		f(c)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(50 * time.Millisecond):
		}
	}

	return nil
}

func newRetrievalProvider(ctx context.Context, r *ribs) *retrievalProvider {
	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		panic(err)
	}
	// TODO defer closer() more better
	go func() {
		<-ctx.Done()
		closer()
	}()

	rp := &retrievalProvider{
		r: r,

		requests: map[mhStr]map[iface.GroupKey]int{},
		gw:       gw,

		attempts: map[peer.ID]int64{},
		fails:    map[peer.ID]int64{},
	}

	lsi, err := lassie.NewLassie(ctx, lassie.WithFinder(rp), lassie.WithConcurrentSPRetrievals(10), lassie.WithGlobalTimeout(30*time.Second), lassie.WithProviderTimeout(4*time.Second))
	if err != nil {
		log.Fatalw("failed to create lassie", "error", err)
	}

	rp.lsi = lsi

	return rp
}

var selectOne = func() ipld.Node {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return ssb.Matcher().Node()
}()

func (r *retrievalProvider) FetchBlocks(ctx context.Context, group iface.GroupKey, mh []multihash.Multihash, cb func(cidx int, data []byte)) error {
	r.reqSourcesLk.Lock()
	for _, m := range mh {
		if _, ok := r.requests[mhStr(m)]; !ok {
			r.requests[mhStr(m)] = map[iface.GroupKey]int{}
		}

		r.requests[mhStr(m)][group]++
	}
	r.reqSourcesLk.Unlock()

	defer func() {
		r.reqSourcesLk.Lock()
		for _, m := range mh {
			r.requests[mhStr(m)][group]--
			if r.requests[mhStr(m)][group] == 0 {
				delete(r.requests[mhStr(m)], group)
			}
		}
		r.reqSourcesLk.Unlock()
	}()

	// todo this is probably horribly inefficient, optimize

	wstor := &ributil.IpldStoreWrapper{BS: blockstore.NewMemorySync()}

	linkSystem := cidlink.DefaultLinkSystem()
	linkSystem.SetWriteStorage(wstor)
	linkSystem.SetReadStorage(wstor)
	linkSystem.TrustedStorage = true
	unixfsnode.AddUnixFSReificationToLinkSystem(&linkSystem)

	for i, hashToGet := range mh {
		cidToGet := cid.NewCidV1(cid.Raw, hashToGet)

		request := types.RetrievalRequest{
			RetrievalID:       must.One(types.NewRetrievalID()),
			Cid:               cidToGet,
			LinkSystem:        linkSystem,
			PreloadLinkSystem: linkSystem,
			Selector:          selectOne,
			Protocols:         []multicodec.Code{multicodec.TransportGraphsyncFilecoinv1},
			MaxBlocks:         10,
		}

		stat, err := r.lsi.Fetch(ctx, request, func(event types.RetrievalEvent) {
			if event.Code() == types.StartedCode && event.StorageProviderId() != "" {
				r.statLk.Lock()
				r.attempts[event.StorageProviderId()]++
				r.statLk.Unlock()
			}
			if event.Code() == types.FailedCode && event.StorageProviderId() != "" {
				log.Errorw("retrieval failed", "cid", cidToGet, "event", event)
				r.statLk.Lock()
				r.fails[event.StorageProviderId()]++
				r.statLk.Unlock()
			}
		})
		if err != nil {
			return xerrors.Errorf("failed to fetch %s: %w", cidToGet, err)
		}

		log.Errorw("retr stat", "dur", stat.Duration, "size", stat.Size, "cid", cidToGet, "provider", stat.StorageProviderId)

		b, err := wstor.BS.Get(ctx, cidToGet)
		if err != nil {
			return xerrors.Errorf("failed to get block from retrieval store: %w", err)
		}

		cb(i, b.RawData())
	}

	return nil
}
