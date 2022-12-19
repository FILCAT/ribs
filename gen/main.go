package main

import (
	"fmt"
	"os"

	gen "github.com/whyrusleeping/cbor-gen"

	"github.com/magik6k/carsplit/ribs/jbob"
)

func main() {
	err := gen.WriteTupleEncodersToFile("./ribs/jbob/cbor_gen.go", "jbob", jbob.Head{})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
