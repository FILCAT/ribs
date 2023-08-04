package rbdeal

import (
	"context"
	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/net/host"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/lib/must"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
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
	"math/rand"
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

	addrLk sync.Mutex
	addrs  map[int64]ProviderAddrInfo

	statLk   sync.Mutex
	attempts map[peer.ID]int64
	fails    map[peer.ID]int64
	success  map[peer.ID]int64

	ongoingRequestsLk sync.Mutex
	ongoingRequests   map[cid.Cid]requestPromise

	blockCache *lru.Cache[mhStr, []byte] // todo 2q with large ghost cache?
}

const BlockCacheSizeMiB = 512
const AvgBlockSize = 256 << 10
const BlockCacheSize = BlockCacheSizeMiB << 20 / AvgBlockSize

type requestPromise struct {
	done chan struct{}
	res  []byte
	err  error
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

	log.Errorw("got retrieval candidates", "cid", cid, "candidates", len(candidates))

	cs := make([]types.RetrievalCandidate, 0, len(candidates))

	for _, candidate := range candidates {
		var addrInfo ProviderAddrInfo
		r.addrLk.Lock()
		if _, ok := r.addrs[candidate.Provider]; !ok {
			ai, err := r.r.db.GetProviderAddrs(candidate.Provider)
			if err != nil {
				r.addrLk.Unlock()
				log.Errorw("failed to get provider addrs", "provider", candidate.Provider, "err", err)
				continue
			}

			r.addrs[candidate.Provider] = ai
		}
		addrInfo = r.addrs[candidate.Provider]
		r.addrLk.Unlock()

		if len(addrInfo.BitswapMaddrs) > 0 {
			log.Errorw("candidate has bitswap addrs", "provider", candidate.Provider)

			bsAddrInfo, err := peer.AddrInfosFromP2pAddrs(addrInfo.BitswapMaddrs...)
			if err != nil {
				log.Errorw("failed to bitswap parse addrinfo", "provider", candidate.Provider, "err", err)
				continue
			}

			for _, ai := range bsAddrInfo {
				cs = append(cs, types.RetrievalCandidate{
					MinerPeer: ai,
					RootCid:   cid,
					Metadata:  metadata.Default.New(&metadata.Bitswap{}),
				})
			}
		}

		gsAddrInfo, err := peer.AddrInfosFromP2pAddrs(addrInfo.LibP2PMaddrs...)
		if err != nil {
			log.Errorw("failed to parse addrinfo", "provider", candidate.Provider, "err", err)
			continue
		}

		if len(gsAddrInfo) == 0 {
			log.Errorw("no gs addrinfo", "provider", candidate.Provider)
			continue
		}

		cs = append(cs, types.RetrievalCandidate{
			MinerPeer: gsAddrInfo[0],
			RootCid:   cid,
			Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{
				PieceCID:      gm.PieceCid,
				VerifiedDeal:  candidate.Verified,
				FastRetrieval: candidate.FastRetr,
			}),
		})
	}

	r.statLk.Lock()
	/*sort.SliceStable(cs, func(i, j int) bool {
		if cs[i].MinerPeer.ID == cs[j].MinerPeer.ID {
			return true
		}

		iattempts := r.attempts[cs[i].MinerPeer.ID]
		jattempts := r.success[cs[j].MinerPeer.ID]

		ifails := r.fails[cs[i].MinerPeer.ID]
		jfails := r.fails[cs[j].MinerPeer.ID]

		ifailRatio := float64(ifails) / float64(iattempts+1)
		jfailRatio := float64(jfails) / float64(jattempts+1)

		if ifailRatio == jfailRatio {
			// prefer bitswap
			if cs[i].Metadata.Protocols()[0] == multicodec.TransportBitswap {
				return true
			}
		}

		return ifailRatio < jfailRatio // prefer peers that have failed less
	})*/

	rand.Shuffle(len(cs), func(i, j int) { cs[i], cs[j] = cs[j], cs[i] })
	r.statLk.Unlock()

	/*n := len(cs)
	if n > 6 { // only return the top 6
		n = 6
	}*/

	for _, c := range cs /*[:n]*/ {
		r.statLk.Lock()
		log.Errorw("select", "p", c.MinerPeer.ID, "tpt", c.Metadata.Protocols()[0].String(), "attempts", r.attempts[c.MinerPeer.ID], "fails", r.fails[c.MinerPeer.ID], "success", r.success[c.MinerPeer.ID])
		r.statLk.Unlock()

		f(c)

		/*select {
		case <-ctx.Done():
			return nil
		case <-time.After(100 * time.Millisecond):
		}*/
	}

	return nil
}

func newRetrievalProvider(ctx context.Context, r *ribs) (*retrievalProvider, error) {
	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		return nil, xerrors.Errorf("create retrieval gateway rpc: %w", err)
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
		success:  map[peer.ID]int64{},

		addrs: map[int64]ProviderAddrInfo{},

		ongoingRequests: map[cid.Cid]requestPromise{},

		blockCache: must.One(lru.New[mhStr, []byte](BlockCacheSize)),
	}

	retrHost, err := host.InitHost(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("init lassie host: %w", err)
	}
	r.retrHost = retrHost

	lsi, err := lassie.NewLassie(ctx,
		lassie.WithFinder(rp),
		lassie.WithConcurrentSPRetrievals(50),
		lassie.WithBitswapConcurrency(50),
		lassie.WithGlobalTimeout(30*time.Second),
		lassie.WithProviderTimeout(4*time.Second),
		lassie.WithHost(retrHost))
	if err != nil {
		return nil, xerrors.Errorf("failed to create lassie: %w", err)
	}

	rp.lsi = lsi

	return rp, nil
}

var selectOne = func() ipld.Node {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return ssb.Matcher().Node()
}()

func (r *retrievalProvider) FetchBlocks(ctx context.Context, group iface.GroupKey, mh []multihash.Multihash, cb func(cidx int, data []byte)) error {
	var cacheHits int
	var bytesServed int64

	defer func() {
		r.r.retrBytes.Add(bytesServed)
	}()

	for i, m := range mh {
		if b, ok := r.blockCache.Get(mhStr(m)); ok {
			cb(i, b)
			cacheHits++
			bytesServed += int64(len(b))
			mh[i] = nil
		}
	}

	r.r.retrCacheHit.Add(int64(cacheHits))
	r.r.retrCacheMiss.Add(int64(len(mh) - cacheHits))
	r.r.retrSuccess.Add(int64(cacheHits))

	if cacheHits == len(mh) {
		return nil
	}

	r.reqSourcesLk.Lock()
	for _, m := range mh {
		if m == nil {
			continue
		}

		if _, ok := r.requests[mhStr(m)]; !ok {
			r.requests[mhStr(m)] = map[iface.GroupKey]int{}
		}

		r.requests[mhStr(m)][group]++
	}
	r.reqSourcesLk.Unlock()

	defer func() {
		r.reqSourcesLk.Lock()
		for _, m := range mh {
			if m == nil {
				continue
			}

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
		if hashToGet == nil {
			continue
		}

		var err error
		for j := 0; j < 16; j++ {
			err = r.fetchOne(ctx, hashToGet, i, linkSystem, wstor, cb, &bytesServed)
			if err == nil {
				break
			}
			log.Errorw("failed to fetch block", "error", err, "attempt", j, "hash", hashToGet)
		}
		if err != nil {
			return xerrors.Errorf("fetchOne: %w", err)
		}
	}

	return nil
}

func (r *retrievalProvider) fetchOne(ctx context.Context, hashToGet multihash.Multihash, i int, linkSystem linking.LinkSystem, wstor *ributil.IpldStoreWrapper, cb func(cidx int, data []byte), bytesServed *int64) error {
	cidToGet := cid.NewCidV1(cid.Raw, hashToGet)

	r.ongoingRequestsLk.Lock()

	if or, ok := r.ongoingRequests[cidToGet]; ok {
		r.ongoingRequestsLk.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-or.done:
		}

		if or.err != nil {
			return xerrors.Errorf("retr promise error: %w", or.err)
		}

		cb(i, or.res)
		return nil
	}

	promise := requestPromise{
		done: make(chan struct{}),
	}

	r.ongoingRequests[cidToGet] = promise
	r.ongoingRequestsLk.Unlock()

	r.r.retrActive.Add(1)
	defer r.r.retrActive.Add(-1)

	request := types.RetrievalRequest{
		RetrievalID:       must.One(types.NewRetrievalID()),
		Cid:               cidToGet,
		LinkSystem:        linkSystem,
		PreloadLinkSystem: linkSystem,
		Selector:          selectOne,
		Protocols:         []multicodec.Code{multicodec.TransportBitswap, multicodec.TransportGraphsyncFilecoinv1},
		MaxBlocks:         10,
	}

	stat, err := r.lsi.Fetch(ctx, request, func(event types.RetrievalEvent) {
		log.Errorw("retrieval event", "cid", cidToGet, "event", event)

		/*if event.Code() == types.StartedCode && event.StorageProviderId() != "" {
			r.statLk.Lock()
			r.attempts[event.StorageProviderId()]++
			r.statLk.Unlock()
		}*/
		if event.Code() == types.FailedCode /* && event.StorageProviderId() != "" */ {
			log.Errorw("RETR ERROR", "cid", cidToGet, "event", event)
			/*r.statLk.Lock()
			r.fails[event.StorageProviderId()]++
			r.statLk.Unlock()*/
		}
		/*if event.Code() == types.SuccessCode && event.StorageProviderId() != "" {
			r.statLk.Lock()
			r.success[event.StorageProviderId()]++
			r.statLk.Unlock()
		}*/
	})
	if err != nil {
		log.Errorw("retrieval failed", "cid", cidToGet, "error", err)

		r.ongoingRequestsLk.Lock()
		delete(r.ongoingRequests, cidToGet)
		r.ongoingRequestsLk.Unlock()

		promise.err = err
		close(promise.done)

		r.r.retrFail.Add(1)

		return xerrors.Errorf("failed to fetch %s: %w", cidToGet, err)
	}

	log.Errorw("retr stat", "dur", stat.Duration, "size", stat.Size, "cid", cidToGet, "provider", stat.StorageProviderId)

	b, err := wstor.BS.Get(ctx, cidToGet)
	if err != nil {
		log.Errorw("failed to get block from retrieval store", "error", err)

		r.ongoingRequestsLk.Lock()
		delete(r.ongoingRequests, cidToGet)
		r.ongoingRequestsLk.Unlock()

		promise.err = err
		close(promise.done)

		r.r.retrFail.Add(1)

		return xerrors.Errorf("failed to get block from retrieval store: %w", err)
	}

	r.ongoingRequestsLk.Lock()
	delete(r.ongoingRequests, cidToGet)
	r.ongoingRequestsLk.Unlock()

	r.blockCache.Add(mhStr(hashToGet), b.RawData())

	promise.res = b.RawData()
	close(promise.done)

	r.r.retrSuccess.Add(1)
	*bytesServed += int64(len(b.RawData()))
	cb(i, b.RawData())

	return nil
}
