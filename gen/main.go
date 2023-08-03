package main

import (
	"fmt"
	"github.com/lotus-web3/ribs/bsst"
	"github.com/lotus-web3/ribs/carlog"
	"github.com/lotus-web3/ribs/ributil/gslite"
	"os"

	gen "github.com/whyrusleeping/cbor-gen"
)

func main() {
	err := gen.WriteMapEncodersToFile("./carlog/cbor_gen.go", "carlog", carlog.Head{})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = gen.WriteMapEncodersToFile("./bsst/cbor_gen.go", "bsst", bsst.BSSTHeader{})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = gen.WriteMapEncodersToFile("./ributil/gslite/cbor_gen.go", "gslite",
		gslite.GraphSyncMessageRoot{},
		gslite.GraphSyncMessage{},
		gslite.GraphSyncRequest{},
		gslite.GraphSyncResponse{},
		gslite.GraphSyncBlock{},
		gslite.GraphSyncLinkMetadatum{})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
