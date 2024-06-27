package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/atboosty/ribs/carlog"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var headCmd = &cli.Command{
	Name:  "head",
	Usage: "Head commands",
	Subcommands: []*cli.Command{
		headToJsonCmd,
		fromJsonCmd,
	},
}

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

var fromJsonCmd = &cli.Command{
	Name:      "from-json",
	Usage:     "write json from stdin into a head file",
	ArgsUsage: "[output head file]",
	Action: func(c *cli.Context) error {
		if c.NArg() != 1 {
			return cli.Exit("Invalid number of arguments. Requires only output head file.", 1)
		}

		// Read the entire JSON from stdin
		jsonData, err := io.ReadAll(os.Stdin)
		if err != nil {
			return xerrors.Errorf("read json from stdin: %w", err)
		}

		var h carlog.Head
		if err := json.Unmarshal(jsonData, &h); err != nil {
			return xerrors.Errorf("unmarshal json: %w", err)
		}

		// Convert struct to CBOR format
		var buf bytes.Buffer
		if err := h.MarshalCBOR(&buf); err != nil {
			return xerrors.Errorf("marshal to cbor: %w", err)
		}

		// Open the head file for writing
		headFile, err := os.Create(c.Args().Get(0))
		if err != nil {
			return xerrors.Errorf("open head file for writing: %w", err)
		}
		defer headFile.Close()

		// Write CBOR data to the head file
		if _, err := headFile.Write(buf.Bytes()); err != nil {
			return xerrors.Errorf("write to head file: %w", err)
		}

		pad := 512 - buf.Len()
		if pad < 0 {
			return xerrors.Errorf("head file too large")
		}

		// Pad the head file to 512 bytes
		if _, err := headFile.Write(make([]byte, pad)); err != nil {
			return xerrors.Errorf("pad head file: %w", err)
		}

		fmt.Printf("Successfully written to %s\n", c.Args().Get(0))

		return nil
	},
}
