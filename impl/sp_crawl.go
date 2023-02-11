package impl

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/actors/adt"
	"github.com/filecoin-project/lotus/chain/actors/builtin/market"
	cbor "github.com/ipfs/go-ipld-cbor"
)

var (
	crawlInit        = "init"
	crawlLoadMarket  = "loading market actor"
	crawlMarketList  = "listing market participants"
	crawlStoreMarket = "storing market participants"
)

func (r *ribs) spCrawler() {
	r.crawlState.Store(&crawlInit)

	defer close(r.spCrawlClosed)

	ctx := context.TODO()

	gw, closer, err := client.NewGatewayRPCV1(ctx, "http://api.chain.love/rpc/v1", nil)
	if err != nil {
		panic(err)
	}
	defer closer()

	for {
		select {
		case <-r.spCrawlClose:
			return
		default:
		}

		r.crawlState.Store(&crawlLoadMarket)

		head, err := gw.ChainHead(ctx)
		if err != nil {
			panic(err)
		}

		mktAct, err := gw.StateGetActor(ctx, market.Address, head.Key())
		if err != nil {
			panic(err)
		}

		stor := adt.WrapStore(ctx, cbor.NewCborStore(blockstore.NewAPIBlockstore(gw)))

		mact, err := market.Load(stor, mktAct)
		if err != nil {
			panic(err)
		}

		bt, err := mact.LockedTable()
		if err != nil {
			panic(err)
		}

		actors := make([]int64, 0)

		n := 0

		err = bt.ForEach(func(k address.Address, v abi.TokenAmount) error {
			i, err := address.IDFromAddress(k)
			if err != nil {
				return err
			}
			actors = append(actors, int64(i))

			n++
			if n%10 == 0 {
				r.crawlState.Store(toPtr(fmt.Sprintf("<b>%s</b><div>At <b>%d</b></div>", crawlMarketList, n))) // todo use json
			}

			return nil
		})
		if err != nil {
			panic(err)
		}

		r.crawlState.Store(&crawlStoreMarket)

		if err := r.db.UpsertMarketActors(actors); err != nil {
			panic(err)
		}

	}
}

func toPtr[T any](v T) *T {
	return &v
}
