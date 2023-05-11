package rbdeal

/*import (
	"context"

	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type ProbingRetrievalFinder struct {
}

func (p *ProbingRetrievalFinder) FindCandidates(ctx context.Context, cid cid.Cid) ([]types.RetrievalCandidate, error) {
	//TODO implement me
	panic("implement me")
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
	rf := &ProbingRetrievalFinder{}

	lassie, err := lassie.NewLassie(ctx, lassie.WithHost(r.host), lassie.WithProviderAllowList(map[peer.ID]bool{}),
		lassie.WithFinder(rf))
	if err != nil {
		log.Fatalw("failed to create lassie", "error", err)
	}

	for {
		// todo get sealed deals from db that weren't checked in the last X time (initially 10 minutes)

		// foreach:

		// get sample cids from group

		// get a sample cid uniqueish to the deal

		// put in probing finder

		// attempt retrieval

		// record success/failure

		if err != nil {
			return
		}
	}
}
*/
