package main

import (
	"fmt"
	"github.com/lotus-web3/ribs/bsst"
	"github.com/lotus-web3/ribs/carlog"
	"os"

	gen "github.com/whyrusleeping/cbor-gen"
)

func main() {
	err := gen.WriteTupleEncodersToFile("./carlog/cbor_gen.go", "jbob", carlog.Head{})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = gen.WriteMapEncodersToFile("./bsst/cbor_gen.go", "bsst", bsst.BSSTHeader{})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
