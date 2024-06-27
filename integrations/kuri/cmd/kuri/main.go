package main

import (
	"os"

	"github.com/ipfs/kubo/cmd/ipfs/kubo"
	"github.com/ipfs/kubo/plugin/loader"

	"github.com/atboosty/ribs/ributil"
	kuboribs "github.com/lotus_web3/ribs/integrations/kuri/ribsplugin"
)

func main() {
	os.Exit(mainRet())
}

func mainRet() (exitCode int) {
	mw := ributil.MemoryWatchdog()
	defer mw()

	return kubo.Start(kubo.BuildEnv(func(loader *loader.PluginLoader) error {
		return loader.Load(kuboribs.Plugin)
	}))
}
