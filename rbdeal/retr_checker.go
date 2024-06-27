package rbdeal

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/atboosty/ribs/carlog"
	lru "github.com/hashicorp/golang-lru/v2"
	trustlessutils "github.com/ipld/go-trustless-utils"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/multiformats/go-multiaddr"

	iface "github.com/atboosty/ribs"
	"github.com/atboosty/ribs/ributil"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/lib/must"
	"github.com/ipfs/go-unixfsnode"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/metadata"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

var retrievalCheckTimeout = 30 * time.Second
var maxConsecutiveTimeouts = 5
var consecutiveTimoutsForgivePeriod = 10 * time.Minute
var parallelChecks = 30

type ProbingRetrievalFinder struct {
	lk      sync.Mutex
	lookups map[cid.Cid][]types.RetrievalCandidate
}

func (p *ProbingRetrievalFinder) FindCandidates(ctx context.Context, cid cid.Cid, f func(types.RetrievalCandidate)) error {
	p.lk.Lock()
	defer p.lk.Unlock()

	lu, ok := p.lookups[cid]
	if !ok {
		log.Errorw("no candidate for cid", "cid", cid)
		return nil
	}

	for _, candidate := range lu {
		f(candidate)
	}

	return nil
}

func (r *ribs) retrievalChecker(ctx context.Context) {
	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		panic(err)
	}
	defer closer()

	rf := &ProbingRetrievalFinder{
		lookups: map[cid.Cid][]types.RetrievalCandidate{},
	}

	lsi, err := lassie.NewLassie(ctx, lassie.WithProviderAllowList(map[peer.ID]bool{}),
		lassie.WithCandidateSource(rf),
		lassie.WithGlobalTimeout(retrievalCheckTimeout),
		lassie.WithProviderTimeout(retrievalCheckTimeout))
	if err != nil {
		log.Fatalw("failed to create lassie", "error", err)
	}

	for {
		err := r.doRetrievalCheck(ctx, gw, rf, lsi)
		if err != nil {
			log.Errorw("failed to do retrieval check", "error", err)
		}

		time.Sleep(15 * time.Minute)
	}
}

type timeoutEntry struct {
	lastTimeout         time.Time
	consecutiveTimeouts int
}

var timeoutLk sync.Mutex

func (r *ribs) doRetrievalCheck(ctx context.Context, gw api.Gateway, prf *ProbingRetrievalFinder, lsi *lassie.Lassie) error {
	candidates, err := r.db.GetRetrievalCheckCandidates()
	if err != nil {
		return xerrors.Errorf("failed to get retrieval check candidates: %w", err)
	}

	r.rckToDo.Store(int64(len(candidates)))
	r.rckStarted.Store(0)
	r.rckSuccess.Store(0)
	r.rckFail.Store(0)

	// last retr check candidates

	groups := map[iface.GroupKey]iface.GroupDesc{}
	samples := map[iface.GroupKey][]multihash.Multihash{}
	for _, candidate := range candidates {
		if _, ok := groups[candidate.Group]; ok {
			continue
		}

		gm, err := r.Storage().DescibeGroup(ctx, candidate.Group)
		if err != nil {
			return xerrors.Errorf("failed to get group meta: %w", err)
		}

		groups[candidate.Group] = gm

		sample, err := r.Storage().HashSample(ctx, candidate.Group)
		if err != nil {
			return xerrors.Errorf("failed to load sample for group %d: %w", candidate.Group, err)
		}

		samples[candidate.Group] = sample
	}

	timeoutCache := must.One(lru.New[int64, *timeoutEntry](1000))

	checkThrottle := make(chan struct{}, parallelChecks)

	for _, candidate := range candidates {
		candidate := candidate

		r.rckStarted.Add(1)

		timeoutLk.Lock()

		v, ok := timeoutCache.Get(candidate.Provider)
		if ok {
			if v.lastTimeout.Add(consecutiveTimoutsForgivePeriod).Before(time.Now()) {
				v.consecutiveTimeouts = 0 // forgive
			}
			if v.consecutiveTimeouts >= maxConsecutiveTimeouts {
				log.Errorw("skipping provider due to consecutive timeouts", "provider", candidate.Provider, "group", candidate.Group, "deal", candidate.DealID)

				res := RetrievalResult{
					Success:         false,
					Error:           fmt.Sprintf("skipped due to %d consecutive timeouts", v.consecutiveTimeouts),
					Duration:        time.Second,
					TimeToFirstByte: 0,
				}

				_ = res
				// don't record for now, just skip trying
				/*err = r.db.RecordRetrievalCheckResult(candidate.DealID, res)
				if err != nil {
					return xerrors.Errorf("failed to record retrieval check result: %w", err)
				}*/

				r.rckFail.Add(1)
				r.rckFailAll.Add(1)

				timeoutLk.Unlock()
				continue
			}
		}

		timeoutLk.Unlock()

	retryGetSample:
		hashToGet := samples[candidate.Group][rand.Intn(len(samples[candidate.Group]))]
		cidToGet := cid.NewCidV1(cid.Raw, hashToGet)

		prf.lk.Lock()
		_, ok = prf.lookups[cidToGet]
		prf.lk.Unlock()
		if ok {
			time.Sleep(1 * time.Millisecond)
			goto retryGetSample
		}

		group := groups[candidate.Group]

		addrInfo, err := r.db.GetProviderAddrs(candidate.Provider)
		if err != nil {
			log.Errorw("failed to get addr info", "error", err)
			r.rckFail.Add(1)
			r.rckFailAll.Add(1)
			continue
		}

		////

		cs := make([]types.RetrievalCandidate, 0, len(candidates))
		var fixedPeer []peer.AddrInfo
		{
			/*if len(addrInfo.HttpMaddrs) > 0 {
				log.Errorw("candidate has http addrs", "provider", candidate.Provider)

				cs = append(cs, types.RetrievalCandidate{
					MinerPeer: peer.AddrInfo{
						ID:    "",
						Addrs: addrInfo.HttpMaddrs,
					},
					RootCid:  cidToGet,
					Metadata: metadata.Default.New(&metadata.IpfsGatewayHttp{}),
				})
			}*/
			/*if len(addrInfo.BitswapMaddrs) > 0 {
				bsAddrInfo, err := peer.AddrInfosFromP2pAddrs(addrInfo.BitswapMaddrs...)
				if err != nil {
					log.Errorw("failed to bitswap parse addrinfo", "provider", candidate.Provider, "err", err)
					r.rckFail.Add(1)
					r.rckFailAll.Add(1)
					continue
				}

				for _, ai := range bsAddrInfo {
					cs = append(cs, types.RetrievalCandidate{
						MinerPeer: ai,
						RootCid:   cidToGet,
						Metadata:  metadata.Default.New(&metadata.Bitswap{}),
					})
				}
			}*/

			gsAddrInfo, err := peer.AddrInfosFromP2pAddrs(addrInfo.LibP2PMaddrs...)
			if err != nil {
				log.Errorw("failed to parse addrinfo", "provider", candidate.Provider, "err", err)
				r.rckFail.Add(1)
				r.rckFailAll.Add(1)
				continue
			}

			if len(gsAddrInfo) == 0 {
				log.Errorw("no gs addrinfo", "provider", candidate.Provider)
				r.rckFail.Add(1)
				r.rckFailAll.Add(1)
				continue
			}

			allMaddrs := append([]multiaddr.Multiaddr{}, addrInfo.BitswapMaddrs...)
			allMaddrs = append(allMaddrs, addrInfo.LibP2PMaddrs...)

			fixedPeer, err = peer.AddrInfosFromP2pAddrs(allMaddrs...)
			if err != nil {
				log.Errorw("failed to parse addrinfo", "provider", candidate.Provider, "err", err)
				r.rckFail.Add(1)
				r.rckFailAll.Add(1)
				continue
			}

			cs = append(cs, types.RetrievalCandidate{
				MinerPeer: gsAddrInfo[0],
				RootCid:   cidToGet,
				Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{
					PieceCID:      group.PieceCid,
					VerifiedDeal:  candidate.Verified,
					FastRetrieval: candidate.FastRetr,
				}),
			})
		}

		checkThrottle <- struct{}{}
		go func() {
			defer func() {
				<-checkThrottle
			}()
			err = r.retrievalCheckCandidate(ctx, candidate, addrInfo, cidToGet, group, fixedPeer, prf, lsi, timeoutCache, cs)
			if err != nil {
				log.Errorw("failed to check candidate", "error", err)
			}
		}()
	}

	for i := 0; i < parallelChecks; i++ {
		checkThrottle <- struct{}{}
	}

	return nil
}

func (r *ribs) retrievalCheckCandidate(ctx context.Context, candidate RetrCheckCandidate, addrInfo ProviderAddrInfo, cidToGet cid.Cid, group iface.GroupDesc, fixedPeer []peer.AddrInfo,
	prf *ProbingRetrievalFinder, lsi *lassie.Lassie, timeoutCache *lru.Cache[int64, *timeoutEntry], cs []types.RetrievalCandidate) error {
	//// http path, maybe
	if len(addrInfo.HttpMaddrs) > 0 {
		u, err := ributil.MaddrsToUrl(addrInfo.HttpMaddrs)
		if err != nil {
			log.Errorw("failed to parse addrinfo", "provider", candidate.Provider, "err", err)
			goto lassie
		}

		start := time.Now()
		var firstByte time.Time

		err = func() error {
			ctx, cancel := context.WithTimeout(ctx, retrievalCheckTimeout)
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, "GET", u.String()+"/ipfs/"+cidToGet.String(), nil)
			if err != nil {
				cancel()
				return xerrors.Errorf("failed to create request: %w", err)
			}

			req.Header.Set("Accept", "application/vnd.ipld.raw;")
			req.Header.Set("User-Agent", "ribs/0.0.0")

			resp, err := http.DefaultClient.Do(req) // todo use a tuned client
			if err != nil {
				log.Errorw("http retrieval failed", "error", err, "url", u.String()+"/ipfs/"+cidToGet.String(), "group", group, "provider", candidate.Provider)
				return xerrors.Errorf("failed to do request: %w", err)
			}

			firstByte = time.Now()

			if resp.StatusCode != 200 {
				_ = resp.Body.Close()
				log.Errorw("http retrieval failed (non-200 response)", "status", resp.StatusCode, "url", u.String()+"/ipfs/"+cidToGet.String(), "group", group, "provider", candidate.Provider)
				return xerrors.Errorf("non-200 response: %d", resp.StatusCode)
			}

			// read and validate block
			/*if carlog.MaxEntryLen < resp.ContentLength {
				_ = resp.Body.Close()
				log.Errorw("http retrieval failed (response too large)", "size", resp.ContentLength, "url", u.String()+"/ipfs/"+cidToGet.String(), "group", group, "provider", candidate.Provider)
				return xerrors.Errorf("response too large: %d", resp.ContentLength)
			}

			if resp.ContentLength < 0 {
				_ = resp.Body.Close()
				log.Errorw("http retrieval failed (response has no content length)", "url", u.String()+"/ipfs/"+cidToGet.String(), "group", group, "provider", candidate.Provider)
				return xerrors.Errorf("response has no content length, or bad content length: %d", resp.ContentLength)
			}*/

			//bbuf := pool.Get(int(resp.ContentLength)) todo not easy because promise stuff
			//buf := make([]byte, carlog.MaxEntryLen)
			bbuf := pool.Get(carlog.MaxEntryLen)
			defer pool.Put(bbuf)

			n, err := io.ReadFull(resp.Body, bbuf)
			if err != nil && err != io.ErrUnexpectedEOF {
				_ = resp.Body.Close()
				log.Errorw("http retrieval failed (failed to read response)", "error", err, "url", u.String()+"/ipfs/"+cidToGet.String(), "group", group, "provider", candidate.Provider)
				return xerrors.Errorf("failed to read response: %w", err)
			}
			bbuf = bbuf[:n]

			if err := resp.Body.Close(); err != nil {
				log.Errorw("http retrieval failed (failed to close response)", "error", err, "url", u.String()+"/ipfs/"+cidToGet.String(), "group", group, "provider", candidate.Provider)
				return xerrors.Errorf("failed to close response: %w", err)
			}

			checkCid, err := cidToGet.Prefix().Sum(bbuf)
			if err != nil {
				log.Errorw("http retrieval failed (failed to hash response)", "error", err, "url", u.String()+"/ipfs/"+cidToGet.String(), "group", group, "provider", candidate.Provider)
				return xerrors.Errorf("failed to hash response: %w", err)
			}

			if !checkCid.Equals(cidToGet) {
				log.Errorw("http retrieval failed (response hash mismatch!!!)", "url", u.String()+"/ipfs/"+cidToGet.String(), "group", group, "provider", candidate.Provider, "expected", cidToGet, "actual", checkCid)
				return xerrors.Errorf("response hash mismatch")
			}

			return nil
		}()
		if err != nil {
			log.Errorw("failed to get http", "provider", candidate.Provider, "err", err)
			goto lassie
		}

		// record success
		err = r.db.RecordRetrievalCheckResult(candidate.DealID, RetrievalResult{
			Success:         true,
			Error:           "",
			Duration:        time.Since(start),
			TimeToFirstByte: firstByte.Sub(start),
		})
		if err != nil {
			return xerrors.Errorf("failed to record retrieval check result: %w", err)
		}

		r.rckSuccess.Add(1)
		r.rckSuccessAll.Add(1)

		log.Errorw("http retrieval check success", "provider", candidate.Provider, "group", candidate.Group, "took", time.Since(start))
		return nil
	}
lassie:

	//// lassie path

	prf.lk.Lock()
	prf.lookups[cidToGet] = cs
	prf.lk.Unlock()

	wstor := &ributil.IpldStoreWrapper{BS: blockstore.NewMemorySync()}

	linkSystem := cidlink.DefaultLinkSystem()
	linkSystem.SetWriteStorage(wstor)
	linkSystem.SetReadStorage(wstor)
	linkSystem.TrustedStorage = true
	unixfsnode.AddUnixFSReificationToLinkSystem(&linkSystem)

	request := types.RetrievalRequest{
		RetrievalID:       must.One(types.NewRetrievalID()),
		LinkSystem:        linkSystem,
		PreloadLinkSystem: linkSystem,
		Protocols:         []multicodec.Code{ /*multicodec.TransportBitswap,*/ multicodec.TransportGraphsyncFilecoinv1 /*, multicodec.TransportIpfsGatewayHttp*/},
		MaxBlocks:         10,
		/*Providers: []types.Provider{
			{
				Peer: fixedPeer,
				Protocols: []metadata.Protocol{

				},
			},
		},*/

		Request: trustlessutils.Request{
			Root:       cidToGet,
			Path:       "",
			Scope:      trustlessutils.DagScopeBlock,
			Bytes:      nil,
			Duplicates: false,
		},
	}

	/*request.PreloadLinkSystem = cidlink.DefaultLinkSystem()
	request.PreloadLinkSystem.SetReadStorage(uselessWrapperStore)
	request.PreloadLinkSystem.SetWriteStorage(uselessWrapperStore)
	request.PreloadLinkSystem.TrustedStorage = true*/

	start := time.Now()
	ctx, done := context.WithTimeout(ctx, retrievalCheckTimeout)

	stat, err := lsi.Fetch(ctx, request, types.WithEventsCallback(func(event types.RetrievalEvent) {
		//log.Errorw("retr event", "event", event.String())
	}))

	done()

	var res RetrievalResult
	if err == nil {
		log.Debugw("retrieval stat", "stat", stat, "provider", candidate.Provider, "group", candidate.Group, "deal", candidate.DealID, "took", time.Since(start))
		res.Success = true
		res.Duration = time.Since(start)
		res.TimeToFirstByte = stat.TimeToFirstByte

		r.rckSuccess.Add(1)
		r.rckSuccessAll.Add(1)
	} else {
		log.Errorw("failed to fetch", "error", err, "provider", candidate.Provider, "group", candidate.Group, "deal", candidate.DealID, "took", time.Since(start))
		res.Success = false
		res.Error = err.Error()

		r.rckFail.Add(1)
		r.rckFailAll.Add(1)

		timeoutLk.Lock()
		if time.Since(start) > retrievalCheckTimeout {
			v, ok := timeoutCache.Get(candidate.Provider)
			if !ok {
				v = &timeoutEntry{
					lastTimeout: time.Now(),
				}
			}
			v.consecutiveTimeouts++
			v.lastTimeout = time.Now()
			timeoutCache.Add(candidate.Provider, v)
		} else {
			v, ok := timeoutCache.Peek(candidate.Provider)
			if ok {
				v.consecutiveTimeouts = 0
				timeoutCache.Add(candidate.Provider, v)
			}
		}
		timeoutLk.Unlock()
	}

	prf.lk.Lock()
	delete(prf.lookups, cidToGet)
	prf.lk.Unlock()

	err = r.db.RecordRetrievalCheckResult(candidate.DealID, res)
	if err != nil {
		return xerrors.Errorf("failed to record retrieval check result: %w", err)
	}

	return nil
}
