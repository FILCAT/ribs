package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"github.com/lotus-web3/ribs/carlog"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var headCmd = &cli.Command{
	Name:  "head",
	Usage: "Head commands",
	Subcommands: []*cli.Command{
		headToJsonCmd,
	},
}

// head-to-json
var headToJsonCmd = &cli.Command{
	Name:      "to-json",
	Usage:     "read a head file into a json file",
	ArgsUsage: "[head file]",
	Action: func(c *cli.Context) error {
		if c.NArg() != 1 {
			return cli.Exit("Invalid number of arguments", 1)
		}

		headFile, err := os.Open(c.Args().First())
		if err != nil {
			return xerrors.Errorf("open head file: %w", err)
		}

		// read head
		var headBuf [carlog.HeadSize]byte
		n, err := headFile.ReadAt(headBuf[:], 0)
		if err != nil {
			return xerrors.Errorf("HEAD READ ERROR: %w", err)
		}
		if n != len(headBuf) {
			return xerrors.Errorf("bad head read bytes (%d bytes)", n)
		}

		var h carlog.Head
		if err := h.UnmarshalCBOR(bytes.NewBuffer(headBuf[:])); err != nil {
			return xerrors.Errorf("unmarshal head: %w", err)
		}

		hjson, err := json.MarshalIndent(h, "", "  ")
		if err != nil {
			return xerrors.Errorf("marshal head: %w", err)
		}

		fmt.Println(string(hjson))

		return nil
	},
}
