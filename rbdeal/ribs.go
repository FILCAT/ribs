package rbdeal

import (
	"context"
	"fmt"
	"github.com/fatih/color"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	iface "github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/rbstor"
	"github.com/lotus-web3/ribs/ributil"
	"golang.org/x/xerrors"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var log = logging.Logger("ribs")

type openOptions struct {
	hostGetter func(...libp2p.Option) (host.Host, error)
}

type OpenOption func(*openOptions)

func WithHostGetter(hg func(...libp2p.Option) (host.Host, error)) OpenOption {
	return func(o *openOptions) {
		o.hostGetter = hg
	}
}

type ribs struct {
	iface.RBS
	db *ribsDB

	host   host.Host
	wallet *ributil.LocalWallet

	lotusRPCAddr string

	marketFundsLk        sync.Mutex
	cachedWalletInfo     *iface.WalletInfo
	lastWalletInfoUpdate time.Time

	//

	close chan struct{}
	//workerClosed chan struct{}
	spCrawlClosed     chan struct{}
	marketWatchClosed chan struct{}

	//

	/* sp tracker */
	crawlState atomic.Pointer[iface.CrawlState]

	/* car uploads */
	uploadStats     map[iface.GroupKey]*iface.UploadStats
	uploadStatsSnap map[iface.GroupKey]*iface.UploadStats

	activeUploads map[uuid.UUID]struct{}
	uploadStatsLk sync.Mutex
}

func (r *ribs) Wallet() iface.Wallet {
	return r
}

func Open(root string, opts ...OpenOption) (iface.RIBS, error) {
	if err := os.Mkdir(root, 0755); err != nil && !os.IsExist(err) {
		return nil, xerrors.Errorf("make root dir: %w", err)
	}

	opt := &openOptions{
		hostGetter: libp2p.New,
	}

	for _, o := range opts {
		o(opt)
	}

	db, err := openRibsDB(root)
	if err != nil {
		return nil, xerrors.Errorf("open db: %w", err)
	}

	rbs, err := rbstor.Open(root, rbstor.WithDB(db.db))
	if err != nil {
		return nil, xerrors.Errorf("open RBS: %w", err)
	}

	r := &ribs{
		RBS: rbs,
		db:  db,

		lotusRPCAddr: "https://pac-l-gw.devtty.eu/rpc/v1",

		uploadStats:     map[iface.GroupKey]*iface.UploadStats{},
		uploadStatsSnap: map[iface.GroupKey]*iface.UploadStats{},
		activeUploads:   map[uuid.UUID]struct{}{},

		close: make(chan struct{}),
		//workerClosed: make(chan struct{}),
		spCrawlClosed:     make(chan struct{}),
		marketWatchClosed: make(chan struct{}),
	}

	{
		walletPath := "~/.ribswallet"

		wallet, err := ributil.OpenWallet(walletPath)
		if err != nil {
			return nil, xerrors.Errorf("open wallet: %w", err)
		}

		defWallet, err := wallet.GetDefault()
		if err != nil {
			wl, err := wallet.WalletList(context.TODO())
			if err != nil {
				return nil, xerrors.Errorf("get wallet list: %w", err)
			}

			if len(wl) == 0 {
				a, err := wallet.WalletNew(context.TODO(), "secp256k1")
				if err != nil {
					return nil, xerrors.Errorf("creating wallet: %w", err)
				}

				color.Yellow("--------------------------------------------------------------")
				fmt.Println("CREATED NEW RIBS WALLET")
				fmt.Println("ADDRESS: ", color.GreenString("%s", a))
				fmt.Println("")
				fmt.Printf("BACKUP YOUR WALLET DIRECTORY (%s)\n", walletPath)
				fmt.Println("")
				fmt.Println("Before using RIBS, you must fund your wallet with FIL.")
				fmt.Println("You can also supply it with DataCap if you want to make")
				fmt.Println("FIL+ deals.")
				color.Yellow("--------------------------------------------------------------")

				wl = append(wl, a)
			}

			if len(wl) != 1 {
				return nil, xerrors.Errorf("no default wallet or more than one wallet: %#v", wl)
			}

			if err := wallet.SetDefault(wl[0]); err != nil {
				return nil, xerrors.Errorf("setting default wallet: %w", err)
			}

			defWallet, err = wallet.GetDefault()
			if err != nil {
				return nil, xerrors.Errorf("getting default wallet: %w", err)
			}
		}

		fmt.Println("RIBS Wallet: ", defWallet)

		r.wallet = wallet

		r.host, err = opt.hostGetter()
		if err != nil {
			return nil, xerrors.Errorf("creating host: %w", err)
		}
	}

	go r.spCrawler()
	go r.dealTracker(context.TODO())
	go r.watchMarket(context.TODO())
	if err := r.setupCarServer(context.TODO(), r.host); err != nil {
		return nil, xerrors.Errorf("setup car server: %w", err)
	}

	r.subGroupChanges()

	return r, nil
}

func (r *ribs) subGroupChanges() {
	r.Storage().Subscribe(func(group iface.GroupKey, from, to iface.GroupState) {
		go r.onSub(group, from, to)
	})
}

func (r *ribs) onSub(group iface.GroupKey, from, to iface.GroupState) {
	if to == iface.GroupStateLocalReadyForDeals {
		c, err := r.db.GetNonFailedDealCount(group)
		if err != nil {
			log.Errorf("getting non-failed deal count: %s", err)
			return
		}

		if c >= minimumReplicaCount {
			return
		}

		err = r.makeMoreDeals(context.TODO(), group, r.host, r.wallet)
		if err != nil {
			log.Errorf("starting new deals: %s", err)
		}
	}
}

func (r *ribs) Close() error {
	close(r.close)
	<-r.spCrawlClosed
	<-r.marketWatchClosed

	return r.RBS.Close()
}
