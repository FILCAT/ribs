package kuboribs

import (
	"context"
	"fmt"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/node"
	"github.com/ipfs/kubo/plugin"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/impl"
	ribsbstore "github.com/lotus-web3/ribs/integrations/blockstore"
	"github.com/lotus-web3/ribs/integrations/web"
	"github.com/mitchellh/go-homedir"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"os"
)

var log = logging.Logger("ribsplugin")

var Plugin plugin.Plugin = &ribsPlugin{}

// ribsPlugin is used for testing the fx plugin.
// It merely adds an fx option that logs a debug statement, so we can verify that it works in tests.
type ribsPlugin struct{}

var _ plugin.PluginFx = (*ribsPlugin)(nil)

func (p *ribsPlugin) Name() string {
	return "ribs-bs"
}

func (p *ribsPlugin) Version() string {
	return "0.0.0"
}

func (p *ribsPlugin) Init(env *plugin.Environment) error {
	return nil
}

func (p *ribsPlugin) Options(info core.FXNodeInfo) ([]fx.Option, error) {
	fmt.Println("RIBS OPTIONS PLUGIN")

	opts := info.FXOptions
	opts = append(opts,
		fx.Provide(makeRibs),
		fx.Provide(ribsBlockstores),

		fx.Decorate(func(rbs *ribsbstore.Blockstore) node.BaseBlocks {
			return rbs
		}),
	)
	return opts, nil
}

// node.BaseBlocks, blockstore.Blockstore, blockstore.GCLocker, blockstore.GCBlockstore

type ribsIn struct {
	fx.In

	Lc fx.Lifecycle
	H  host.Host `optional:"true"`
}

var (
	defaultDataDir = "~/.ribsdata"
	dataEnv        = "RIBS_DATA"
)

func makeRibs(ri ribsIn) (ribs.RIBS, error) {
	var opts []impl.OpenOption
	if ri.H != nil {
		opts = append(opts, impl.WithHostGetter(func(...libp2p.Option) (host.Host, error) {
			return ri.H, nil
		}))
	}

	dataDir := os.Getenv(dataEnv)
	if dataDir == "" {
		dataDir = defaultDataDir
	}
	dataDir, err := homedir.Expand(dataDir)
	if err != nil {
		return nil, xerrors.Errorf("expand data dir: %w", err)
	}

	r, err := impl.Open(dataDir, opts...)
	if err != nil {
		return nil, xerrors.Errorf("open ribs: %w", err)
	}

	ri.Lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return r.Close()
		},
	})

	if ri.H != nil {
		go func() {
			if err := web.Serve(context.TODO(), ":9010", r); err != nil {
				panic("ribsweb serve failed")
			}
		}()
		_, _ = fmt.Fprintf(os.Stderr, "RIBSWeb at http://%s\n", "127.0.0.1:9010")
	}

	return r, nil
}

func ribsBlockstores(r ribs.RIBS, lc fx.Lifecycle) *ribsbstore.Blockstore {
	rbs := ribsbstore.New(context.TODO(), r)

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return rbs.Close()
		},
	})

	return rbs
}

type dummyGCLocker struct{}

func (d *dummyGCLocker) Unlock(ctx context.Context) {
	return
}

func (d *dummyGCLocker) GCLock(ctx context.Context) blockstore.Unlocker {
	panic("no gc")
}

func (d *dummyGCLocker) PinLock(ctx context.Context) blockstore.Unlocker {
	return d
}

func (d *dummyGCLocker) GCRequested(ctx context.Context) bool {
	return false
}

var _ blockstore.GCLocker = (*dummyGCLocker)(nil)
