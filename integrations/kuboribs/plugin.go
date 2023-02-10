package kuboribs

import (
	"context"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/node"
	"github.com/ipfs/kubo/plugin"
	"github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/impl"
	ribsbstore "github.com/lotus-web3/ribs/integrations/blockstore"
	"go.uber.org/fx"
)

var log = logging.Logger("ribsplugin")

var Plugins = []plugin.Plugin{
	&ribsPlugin{},
}

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
	opts := info.FXOptions
	opts = append(opts,
		fx.Provide(makeRibs),
		fx.Replace(ribsBlockstores),
	)
	return opts, nil
}

func makeRibs() (ribs.RIBS, error) {
	log.Errorw("MAKE RIBS")

	return impl.Open("/tmp/ribs.db")
}

func ribsBlockstores(r ribs.RIBS) (node.BaseBlocks, blockstore.Blockstore, blockstore.GCLocker, blockstore.GCBlockstore) {
	ribsbs := ribsbstore.New(context.TODO(), r)

	return ribsbs, ribsbs, &dummyGCLocker{}, blockstore.NewGCBlockstore(ribsbs, &dummyGCLocker{})
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
