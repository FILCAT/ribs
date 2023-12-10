package main

import (
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var ldbcidCmd = &cli.Command{
	Name: "ldb-cid",
	Subcommands: []*cli.Command{
		ldbCidReadCmd,
		ldbCidWriteCmd,
	},
}

var ldbCidReadCmd = &cli.Command{
	Name:      "read",
	Usage:     "Read a CID from the local database",
	ArgsUsage: "[leveldb path] [key]",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 2 {
			return xerrors.Errorf("must pass leveldb path and key")
		}

		ldbPath := cctx.Args().Get(0)
		key := cctx.Args().Get(1)

		ldbPath, err := homedir.Expand(ldbPath)
		if err != nil {
			return xerrors.Errorf("expand homedir: %w", err)
		}

		ldb, err := leveldb.NewDatastore(ldbPath, nil)
		if err != nil {
			return xerrors.Errorf("open leveldb: %w", err)
		}

		c, err := ldb.Get(context.Background(), datastore.NewKey(key))
		if err != nil {
			return xerrors.Errorf("get key: %w", err)
		}

		cc, err := cid.Cast(c)
		if err != nil {
			return xerrors.Errorf("cast cid: %w", err)
		}

		fmt.Println(cc.String())

		return nil
	},
}

var ldbCidWriteCmd = &cli.Command{
	Name:      "write",
	Usage:     "Write a CID to the local database",
	ArgsUsage: "[leveldb path] [key] [cid]",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 3 {
			return xerrors.Errorf("must pass leveldb path, key, and cid")
		}

		ldbPath := cctx.Args().Get(0)
		key := cctx.Args().Get(1)
		cidStr := cctx.Args().Get(2)

		ldbPath, err := homedir.Expand(ldbPath)
		if err != nil {
			return xerrors.Errorf("expand homedir: %w", err)
		}

		ldb, err := leveldb.NewDatastore(ldbPath, nil)
		if err != nil {
			return xerrors.Errorf("open leveldb: %w", err)
		}

		{
			// print old

			c, err := ldb.Get(context.Background(), datastore.NewKey(key))
			if err != nil {
				if err == datastore.ErrNotFound {
					goto afterPrint
				}
				return xerrors.Errorf("get key: %w", err)
			}

			cc, err := cid.Cast(c)
			if err != nil {
				return xerrors.Errorf("cast cid: %w", err)
			}

			fmt.Println("Old:", cc.String())
		}
	afterPrint:

		cc, err := cid.Decode(cidStr)
		if err != nil {
			return xerrors.Errorf("decode cid: %w", err)
		}

		if err := ldb.Put(context.Background(), datastore.NewKey(key), cc.Bytes()); err != nil {
			return xerrors.Errorf("put key: %w", err)
		}

		return nil
	},
}
