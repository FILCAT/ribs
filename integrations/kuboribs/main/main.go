package main

import (
	uniquepkgname "github.com/lotus-web3/ribs/integrations/kuboribs"
)

var Plugins = uniquepkgname.Plugins //nolint

func main() {
	panic("this is a plugin, build it as a plugin, see go#20312")
}
