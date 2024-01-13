package rbdeal

import (
	"context"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	trustlessutils "github.com/ipld/go-trustless-utils"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/lib/must"
	"github.com/ipfs/go-unixfsnode"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/metadata"
	iface "github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/ributil"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

var retrievalCheckTimeout = 7 * time.Second
var maxConsecutiveTimeouts = 3
var consecutiveTimoutsForgivePeriod = 15 * time.Minute

type ProbingRetrievalFinder struct {
	lk      sync.Mutex
	lookups map[cid.Cid]types.RetrievalCandidate
}

func (p *ProbingRetrievalFinder) FindCandidates(ctx context.Context, cid cid.Cid) ([]types.RetrievalCandidate, error) {
	p.lk.Lock()
	defer p.lk.Unlock()

	lu, ok := p.lookups[cid]
	if !ok {
		log.Errorw("no candidate for cid", "cid", cid)
		return nil, nil
	}

	return []types.RetrievalCandidate{lu}, nil
}

func (p *ProbingRetrievalFinder) FindCandidatesAsync(ctx context.Context, cid cid.Cid, f func(types.RetrievalCandidate)) error {
	c, err := p.FindCandidates(ctx, cid)
	if err != nil {
		return err
	}

	for _, candidate := range c {
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
		lookups: map[cid.Cid]types.RetrievalCandidate{},
	}

	lsi, err := lassie.NewLassie(ctx, lassie.WithProviderAllowList(map[peer.ID]bool{}),
		lassie.WithFinder(rf),
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

	// sort candidates by sp id
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Provider < candidates[j].Provider
	})

	type timeoutEntry struct {
		lastTimeout         time.Time
		consecutiveTimeouts int
	}

	timeoutCache := must.One(lru.New[int64, *timeoutEntry](1000))
	var timeoutLk sync.Mutex

	for _, candidate := range candidates {
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

				err = r.db.RecordRetrievalCheckResult(candidate.DealID, res)
				if err != nil {
					return xerrors.Errorf("failed to record retrieval check result: %w", err)
				}

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

		maddr, err := address.NewIDAddress(uint64(candidate.Provider))
		if err != nil {
			return xerrors.Errorf("new id address: %w", err)
		}

		addrInfo, err := GetAddrInfo(ctx, gw, maddr)
		if err != nil {
			log.Errorw("failed to get addr info", "error", err)
			continue
		}

		cent := types.RetrievalCandidate{
			MinerPeer: *addrInfo,
			RootCid:   cidToGet,
			Metadata: metadata.Default.New(&metadata.GraphsyncFilecoinV1{
				PieceCID:      group.PieceCid,
				VerifiedDeal:  candidate.Verified,
				FastRetrieval: candidate.FastRetr,
			}),
		}

		prf.lk.Lock()
		prf.lookups[cidToGet] = cent
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
			Protocols:         []multicodec.Code{multicodec.TransportGraphsyncFilecoinv1},
			MaxBlocks:         10,
			FixedPeers:        []peer.AddrInfo{*addrInfo},

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

			if time.Since(start) > retrievalCheckTimeout {
				timeoutLk.Lock()
				v, ok := timeoutCache.Get(candidate.Provider)
				if !ok {
					v = &timeoutEntry{
						lastTimeout: time.Now(),
					}
				}
				v.consecutiveTimeouts++
				v.lastTimeout = time.Now()
				timeoutCache.Add(candidate.Provider, v)
				timeoutLk.Unlock()
			}
		}

		prf.lk.Lock()
		delete(prf.lookups, cidToGet)
		prf.lk.Unlock()

		err = r.db.RecordRetrievalCheckResult(candidate.DealID, res)
		if err != nil {
			return xerrors.Errorf("failed to record retrieval check result: %w", err)
		}
	}

	return nil
}
