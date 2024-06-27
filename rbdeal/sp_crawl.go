package rbdeal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/atboosty/ribs/ributil"

	iface "github.com/atboosty/ribs"
	"github.com/atboosty/ribs/ributil/boostnet"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/actors/adt"
	"github.com/filecoin-project/lotus/chain/actors/builtin/market"
	"github.com/filecoin-project/lotus/chain/types"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	inet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"golang.org/x/xerrors"
)

const AskProtocolID = "/fil/storage/ask/1.1.0"

var (
	crawlInit           = "init"
	crawlLoadMarket     = "loading market actor"
	crawlMarketList     = "listing market participants"
	crawlStoreMarket    = "storing market participants"
	crawlQueryProviders = "querying providers"
	crawlIdle           = "idle"
)

var crawlFrequency = 30 * time.Minute

func (r *ribs) setCrawlState(state iface.CrawlState) {
	r.crawlState.Store(&state)
}

func (r *ribs) spCrawler() {
	r.setCrawlState(iface.CrawlState{State: crawlInit})

	defer close(r.spCrawlClosed)

	ctx := context.TODO()

	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		panic(err)
	}
	defer closer()

	pingP2P, err := libp2p.New()
	if err != nil {
		panic(err)
	}
	r.crawlHost = pingP2P

	for {
		select {
		case <-r.close:
			return
		default:
		}

		err := r.spCrawlLoop(ctx, gw, pingP2P)
		if err != nil {
			log.Errorw("sp crawl loop", "err", err)
		}

		r.setCrawlState(iface.CrawlState{State: crawlIdle})

		select {
		case <-r.close:
			return
		case <-time.After(crawlFrequency):
		}
	}
}

func (r *ribs) spCrawlLoop(ctx context.Context, gw api.Gateway, pingP2P host.Host) error {
	boostTptClient := boostnet.NewTransportsClient(pingP2P)

	r.setCrawlState(iface.CrawlState{State: crawlLoadMarket})

	head, err := gw.ChainHead(ctx)
	if err != nil {
		return xerrors.Errorf("getting chain head: %w", err)
	}

	// todo at finality

	mktAct, err := gw.StateGetActor(ctx, market.Address, head.Key())
	if err != nil {
		return xerrors.Errorf("getting market actor: %w", err)
	}

	stor := adt.WrapStore(ctx, cbor.NewCborStore(blockstore.NewAPIBlockstore(gw)))

	mact, err := market.Load(stor, mktAct)
	if err != nil {
		return xerrors.Errorf("loading market actor: %w", err)
	}

	bt, err := mact.LockedTable()
	if err != nil {
		return xerrors.Errorf("getting locked table: %w", err)
	}

	actors := make([]int64, 0)

	n := 0

	err = bt.ForEach(func(k address.Address, v abi.TokenAmount) error {
		i, err := address.IDFromAddress(k)
		if err != nil {
			return err
		}
		actors = append(actors, int64(i))

		// todo fileter out accounts

		n++
		if n%10 == 0 {
			r.setCrawlState(iface.CrawlState{State: crawlMarketList, At: int64(n)})
		}

		select {
		case <-r.close:
			return errors.New("stop")
		default:
		}

		return nil
	})
	if err != nil {
		select {
		case <-r.close:
			return nil
		default:
		}
		return err
	}

	r.setCrawlState(iface.CrawlState{State: crawlStoreMarket})

	if err := r.db.UpsertMarketActors(actors); err != nil {
		return xerrors.Errorf("upserting market actors: %w", err)
	}

	r.setCrawlState(iface.CrawlState{State: crawlQueryProviders})

	const parallel = 128
	throttle := make(chan struct{}, parallel)
	const timeout = time.Second * 8

	var stlk sync.Mutex

	var started, reachable, boost, bitswap, bhttp int64

	for n, actor := range actors {
		select {
		case <-r.close:
			return nil
		default:
		}

		throttle <- struct{}{}
		started++

		if started%10 == 0 {
			r.setCrawlState(iface.CrawlState{
				State:     crawlQueryProviders,
				At:        int64(n),
				Reachable: atomic.LoadInt64(&reachable),
				Total:     int64(len(actors)),
				Boost:     atomic.LoadInt64(&boost),
				BBswap:    atomic.LoadInt64(&bitswap),
				BHttp:     atomic.LoadInt64(&bhttp),
			})
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
					log.Debugw("error querying provider", "actor", actor, "err", err)
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

			libp2pPi, err := GetAddrInfo(ctx, gw, maddr)
			if err != nil {
				return
			}

			if err := pingP2P.Connect(ctx, *libp2pPi); err != nil {
				return
			}

			res.PingOk = true
			atomic.AddInt64(&reachable, 1)

			boostTpt, err := boostTptClient.SendQuery(ctx, libp2pPi.ID)
			if err != nil {
				return
			}

			log.Errorw("got boost tpt", "tpt", boostTpt, "provider", maddr)

			res.BoostDeals = true // todo this is technically not necesarily true, but for now it is good enough
			atomic.AddInt64(&boost, 1)

			for _, protocol := range boostTpt.Protocols {
				publicAddrs := make([]multiaddr.Multiaddr, 0, len(protocol.Addresses))
				for _, ma := range protocol.Addresses {
					if IsPublicOrDNSAddr(ma) {
						publicAddrs = append(publicAddrs, ma)
					}
				}

				if len(publicAddrs) == 0 {
					continue
				}

				protocol.Addresses = publicAddrs

				switch protocol.Name {
				case "libp2p":
					// add to libp2pPi

					for _, ma := range protocol.Addresses {
						var has bool
						for _, a := range libp2pPi.Addrs {
							if a.String() == ma.String() {
								has = true
								break
							}
						}
						if !has {
							libp2pPi.Addrs = append(libp2pPi.Addrs, ma)
						}
					}

				case "http":
					u, err := ributil.MaddrsToUrl(protocol.Addresses)
					if err != nil {
						log.Errorw("error parsing http addrs", "err", err, "provider", maddr, "addrs", protocol.Addresses)
						continue
					}

					// query {url}/info, which returns json like {"Version":"0.3.0"}

					ctx, cancel := context.WithTimeout(ctx, timeout)

					var qres struct {
						Version string
					}
					qurl := *u
					qurl.Path = path.Join(qurl.Path, "info")

					req, err := http.NewRequestWithContext(ctx, "GET", qurl.String(), nil)
					if err != nil {
						log.Errorw("error creating http request", "err", err, "provider", maddr, "url", qurl.String())
						cancel()
						continue
					}

					log.Errorw("pinging http", "provider", maddr, "url", qurl.String())

					resp, err := http.DefaultClient.Do(req)
					if err != nil {
						log.Errorw("error querying http", "err", err, "provider", maddr, "url", qurl.String())
						cancel()
						continue
					}

					if resp.ContentLength > 1024 {
						log.Errorw("http response too large", "provider", maddr, "url", qurl.String(), "size", resp.ContentLength)
						cancel()
						continue
					}

					if err := json.NewDecoder(resp.Body).Decode(&qres); err != nil {
						log.Errorw("error decoding http response", "err", err, "provider", maddr, "url", qurl.String())
						cancel()
						continue
					}

					cancel()

					if qres.Version != "" {
						res.BoosterHttp = true
						res.HttpMaddrs = protocol.Addresses
						atomic.AddInt64(&bhttp, 1)
					}
				case "bitswap":
					bswapPIs, err := peer.AddrInfosFromP2pAddrs(protocol.Addresses...)
					if err != nil {
						log.Errorw("error parsing bitswap addrs", "err", err, "provider", maddr, "addrs", protocol.Addresses)
						continue
					}

					if len(bswapPIs) == 0 {
						continue
					}

					// ping each
					for _, pi := range bswapPIs {
						ctx, cancel := context.WithTimeout(ctx, timeout)

						if err := pingP2P.Connect(ctx, pi); err != nil {
							cancel()
							continue
						}

						resCh := ping.Ping(ctx, pingP2P, pi.ID)
						select {
						case pres := <-resCh:
							if pres.Error == nil {
								res.BoosterBitswap = true
							}
							log.Errorw("pinging bitswap", "err", pres.Error, "provider", maddr, "peer", pi.ID)
						case <-ctx.Done():
							log.Errorw("error pinging bitswap", "err", ctx.Err(), "provider", maddr, "peer", pi.ID)
						}
						cancel()

						// todo check protocols to see if it actually has bitswap

						if res.BoosterBitswap {
							break
						}
					}

					if res.BoosterBitswap {
						res.BitswapMaddrs = protocol.Addresses
						atomic.AddInt64(&bitswap, 1)
					}
				default:
				}
			}

			res.LibP2PMaddrs, err = peer.AddrInfoToP2pAddrs(libp2pPi)
			if err != nil {
				log.Errorw("error converting libp2p pi to addrs", "err", err, "pi", libp2pPi)
				return
			}

			s, err := pingP2P.NewStream(ctx, libp2pPi.ID, AskProtocolID)
			if err != nil {
				return
			}
			defer s.Close()

			var resp network.AskResponse

			askRequest := network.AskRequest{
				Miner: maddr,
			}

			if err := doRpc(ctx, s, &askRequest, &resp); err != nil {
				return
			}

			if resp.Ask == nil {
				log.Errorw("got nil ask", "actor", actor)
				return
			}

			if resp.Ask.Ask == nil {
				log.Errorw("got nil ask", "actor", actor)
				return
			}

			if err := r.db.UpdateProviderStorageAsk(actor, resp.Ask.Ask); err != nil {
				log.Errorw("error updating provider ask", "actor", actor, "err", err)
			}

		}(actor)
	}

	for i := 0; i < parallel; i++ {
		throttle <- struct{}{}
	}

	return nil
}

type providerResult struct {
	PingOk         bool
	BoostDeals     bool
	BoosterHttp    bool
	BoosterBitswap bool

	LibP2PMaddrs  []multiaddr.Multiaddr
	BitswapMaddrs []multiaddr.Multiaddr
	HttpMaddrs    []multiaddr.Multiaddr
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

func doRpc(ctx context.Context, s inet.Stream, req interface{}, resp interface{}) error {
	errc := make(chan error)
	go func() {
		if err := cborutil.WriteCborRPC(s, req); err != nil {
			errc <- fmt.Errorf("failed to send request: %w", err)
			return
		}

		if err := cborutil.ReadCborRPC(s, resp); err != nil {
			errc <- fmt.Errorf("failed to read response: %w", err)
			return
		}

		errc <- nil
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func IsPublicOrDNSAddr(a multiaddr.Multiaddr) bool {
	if manet.IsPublicAddr(a) {
		return true
	}

	isPublic := false
	multiaddr.ForEach(a, func(c multiaddr.Component) bool {
		switch c.Protocol().Code {
		case multiaddr.P_DNS, multiaddr.P_DNS4, multiaddr.P_DNS6:
			isPublic = true
		}
		return false
	})
	return isPublic
}
