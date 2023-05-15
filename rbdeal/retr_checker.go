package rbdeal

import (
	"context"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/lib/must"
	"github.com/ipfs/go-unixfsnode"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/ipni/go-libipni/metadata"
	iface "github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/ributil"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
	"math/rand"
	"sync"
	"time"

	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

var retrievalCheckTimeout = 45 * time.Second

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
		lassie.WithFinder(rf))
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

	for _, candidate := range candidates {
		hashToGet := samples[candidate.Group][rand.Intn(len(samples[candidate.Group]))]
		cidToGet := cid.NewCidV1(cid.Raw, hashToGet)

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
			RootCid:   group.RootCid,
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

		ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
		rsn := ssb.Matcher().Node()

		request := types.RetrievalRequest{
			RetrievalID:       must.One(types.NewRetrievalID()),
			Cid:               cidToGet,
			LinkSystem:        linkSystem,
			PreloadLinkSystem: linkSystem,
			Selector:          rsn,
			Protocols:         []multicodec.Code{multicodec.TransportGraphsyncFilecoinv1},
			MaxBlocks:         10,
			FixedPeers:        []peer.AddrInfo{*addrInfo},
		}

		/*request.PreloadLinkSystem = cidlink.DefaultLinkSystem()
		request.PreloadLinkSystem.SetReadStorage(uselessWrapperStore)
		request.PreloadLinkSystem.SetWriteStorage(uselessWrapperStore)
		request.PreloadLinkSystem.TrustedStorage = true*/

		start := time.Now()
		ctx, done := context.WithTimeout(ctx, retrievalCheckTimeout)

		stat, err := lsi.Fetch(ctx, request, func(event types.RetrievalEvent) {
			//log.Errorw("retr event", "event", event.String())
		})

		done()

		var res RetrievalResult
		if err == nil {
			log.Errorw("retrieval stat", "stat", stat)
			res.Success = true
			res.Duration = time.Since(start)
			res.TimeToFirstByte = stat.TimeToFirstByte
		} else {
			log.Errorw("failed to fetch", "error", err)
			res.Success = false
			res.Error = err.Error()
		}

		err = r.db.RecordRetrievalCheckResult(candidate.DealID, res)
		if err != nil {
			return xerrors.Errorf("failed to record retrieval check result: %w", err)
		}
	}

	return nil
}
