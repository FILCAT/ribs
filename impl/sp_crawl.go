package impl

import (
	"context"
	"fmt"
	"github.com/filecoin-project/boost/retrievalmarket/lp2pimpl"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/actors/adt"
	"github.com/filecoin-project/lotus/chain/actors/builtin/market"
	"github.com/filecoin-project/lotus/chain/types"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"sync"
	"sync/atomic"
	"time"
)

var (
	crawlInit           = "init"
	crawlLoadMarket     = "loading market actor"
	crawlMarketList     = "listing market participants"
	crawlStoreMarket    = "storing market participants"
	crawlQueryProviders = "querying providers"
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

	pingP2P, err := libp2p.New()
	if err != nil {
		panic(err)
	}

	boostTptClient := lp2pimpl.NewTransportsClient(pingP2P)

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

		r.crawlState.Store(&crawlQueryProviders)

		const parallel = 64
		throttle := make(chan struct{}, parallel)
		const timeout = time.Second * 8

		var stlk sync.Mutex

		var started, reachable, boost, bitswap, http int64

		for _, actor := range actors {
			throttle <- struct{}{}
			started++

			if started%10 == 0 {
				r.crawlState.Store(toPtr(fmt.Sprintf("<b>%s</b><div>At <b>%d</b>; Reachable <b>%d</b>; Boost/BBitswap/BHttp <b>%d/%d/%d</b></div>", crawlQueryProviders, started, atomic.LoadInt64(&reachable), atomic.LoadInt64(&boost), atomic.LoadInt64(&bitswap), atomic.LoadInt64(&http)))) // todo use json
			}

			go func(actor int64) {
				defer func() {
					<-throttle
				}()

				var res providerResult
				var err error

				defer func() {
					stlk.Lock()
					defer stlk.Unlock()

					if err != nil {
						log.Errorw("error querying provider", "actor", actor, "err", err)
					}

					if err := r.db.UpdateProviderProtocols(actor, res); err != nil {
						log.Errorw("error updating provider", "actor", actor, "err", err)
					}
				}()

				maddr, err := address.NewIDAddress(uint64(actor))
				if err != nil {
					return
				}

				ctx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()

				pi, err := GetAddrInfo(ctx, gw, maddr)
				if err != nil {
					return
				}

				if err := pingP2P.Connect(ctx, *pi); err != nil {
					return
				}

				res.PingOk = true
				atomic.AddInt64(&reachable, 1)

				boostTpt, err := boostTptClient.SendQuery(ctx, pi.ID)
				if err != nil {
					return
				}

				res.BoostDeals = true // todo this is technically not necesarily true, but for now it is good enough
				atomic.AddInt64(&boost, 1)

				for _, protocol := range boostTpt.Protocols {
					switch protocol.Name {
					case "libp2p":
					case "http":
						res.BoosterHttp = true
						atomic.AddInt64(&http, 1)
					case "bitswap":
						res.BoosterBitswap = true
						atomic.AddInt64(&bitswap, 1)
					default:
					}
				}

			}(actor)
		}

		for i := 0; i < parallel; i++ {
			throttle <- struct{}{}
		}

	}
}

type providerResult struct {
	PingOk         bool
	BoostDeals     bool
	BoosterHttp    bool
	BoosterBitswap bool
}

func toPtr[T any](v T) *T {
	return &v
}

func GetAddrInfo(ctx context.Context, api api.Gateway, maddr address.Address) (*peer.AddrInfo, error) {
	minfo, err := api.StateMinerInfo(ctx, maddr, types.EmptyTSK)
	if err != nil {
		return nil, err
	}
	if minfo.PeerId == nil {
		return nil, fmt.Errorf("storage provider %s has no peer ID set on-chain", maddr)
	}

	var maddrs []multiaddr.Multiaddr
	for _, mma := range minfo.Multiaddrs {
		ma, err := multiaddr.NewMultiaddrBytes(mma)
		if err != nil {
			return nil, fmt.Errorf("storage provider %s had invalid multiaddrs in their info: %w", maddr, err)
		}
		maddrs = append(maddrs, ma)
	}
	if len(maddrs) == 0 {
		return nil, fmt.Errorf("storage provider %s has no multiaddrs set on-chain", maddr)
	}

	return &peer.AddrInfo{
		ID:    *minfo.PeerId,
		Addrs: maddrs,
	}, nil
}
