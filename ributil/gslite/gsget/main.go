package main

import (
	"context"
	"fmt"
	"github.com/filecoin-project/lotus/lib/addrutil"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	"github.com/lotus-web3/ribs/ributil/gslite"
	"golang.org/x/xerrors"
	"os"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	// [/peer/id] [cid]
	if len(os.Args) != 3 {
		return xerrors.Errorf("expected 2 args: [peer addr] [cid]")
	}

	toFetch, err := cid.Parse(os.Args[2])
	if err != nil {
		return xerrors.Errorf("parsing cid to fetch: %w", err)
	}
	ctx := context.Background()

	pi, err := addrutil.ParseAddresses(ctx, []string{os.Args[1]})
	if err != nil {
		return xerrors.Errorf("parse host address: %w", err)
	}

	if len(pi) != 1 {
		return xerrors.Errorf("expected one peerinfo")
	}

	fmt.Println("starting libp2p")

	h, err := libp2p.New()
	if err != nil {
		return xerrors.Errorf("starting libp2p: %w", err)
	}

	fmt.Println("connecting")

	if err := h.Connect(ctx, pi[0]); err != nil {
		return xerrors.Errorf("connect to host: %w", err)
	}

	gl := gslite.New(h)

	fmt.Println("requesting block")

	b, err := gl.RequestBlock(ctx, pi[0].ID, toFetch)
	if err != nil {
		return err
	}

	fmt.Printf("res: %s\n", b.Cid())
	return nil
}
