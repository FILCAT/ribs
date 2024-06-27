package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"os"

	"github.com/cheggaaa/pb"
	"github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	"github.com/multiformats/go-multihash"

	"github.com/atboosty/ribs/carlog"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var idxLevelCmd = &cli.Command{
	Name:  "idx-level",
	Usage: "Head commands",
	Subcommands: []*cli.Command{
		toTruncateCmd,
		findCmd,
		matchCarlogCids,
		checkOffsetsCmd,
	},
}

var toTruncateCmd = &cli.Command{
	Name:      "to-truncate",
	Usage:     "read a head file into a json file",
	ArgsUsage: "[leveldb file]",
	Flags: []cli.Flag{
		&cli.Int64Flag{
			Name:     "carlog-size",
			Required: true,
		},
		&cli.BoolFlag{
			Name: "cids",
		},
	},
	Action: func(c *cli.Context) error {
		if c.NArg() != 1 {
			return cli.Exit("Invalid number of arguments", 1)
		}

		li, err := carlog.OpenLevelDBIndex(c.Args().First(), false)
		if err != nil {
			return xerrors.Errorf("open leveldb index: %w", err)
		}

		mhs, err := li.ToTruncate(c.Int64("carlog-size"))
		if err != nil {
			return xerrors.Errorf("to truncate: %w", err)
		}

		if !c.Bool("cids") {
			fmt.Println("blocks to truncate:", len(mhs))
		}

		for _, mh := range mhs {
			fmt.Println(cid.NewCidV1(cid.Raw, mh).String())
		}

		return nil
	},
}

var findCmd = &cli.Command{
	Name:      "find",
	Usage:     "get index entry for a cid",
	ArgsUsage: "[leveldb file] [cid]",
	Action: func(c *cli.Context) error {
		if c.NArg() != 2 {
			return cli.Exit("Invalid number of arguments", 1)
		}

		li, err := carlog.OpenLevelDBIndex(c.Args().First(), false)
		if err != nil {
			return xerrors.Errorf("open leveldb index: %w", err)
		}

		ci, err := cid.Parse(c.Args().Get(1))
		if err != nil {
			return xerrors.Errorf("parse cid: %w", err)
		}

		offs, err := li.Get([]multihash.Multihash{ci.Hash()})
		if err != nil {
			return xerrors.Errorf("get: %w", err)
		}

		for _, off := range offs {
			fmt.Println(off)
		}

		return nil
	},
}

var matchCarlogCids = &cli.Command{
	Name:      "match-carlog",
	Usage:     "match carlog cids with leveldb index",
	ArgsUsage: "[carlog file] [leveldb file]",
	Action: func(c *cli.Context) error {
		if c.NArg() != 2 {
			return cli.Exit("Invalid number of arguments", 1)
		}

		li, err := carlog.OpenLevelDBIndex(c.Args().Get(1), false)
		if err != nil {
			return xerrors.Errorf("open leveldb index: %w", err)
		}

		carlogFile, err := os.Open(c.Args().First())
		if err != nil {
			return xerrors.Errorf("open carlog file: %w", err)
		}
		defer carlogFile.Close()

		fileInfo, err := carlogFile.Stat()
		if err != nil {
			return xerrors.Errorf("retrieving file info: %w", err)
		}

		bar := pb.New64(int64(fileInfo.Size())).Start()
		bar.Units = pb.U_BYTES

		br := bufio.NewReader(carlogFile)

		// Read the CAR header
		_, err = car.ReadHeader(br)
		if err != nil {
			return xerrors.Errorf("reading car header: %w", err)
		}

		seenSet, notIndexedSet := cid.NewSet(), cid.NewSet()

		entBuf := make([]byte, 16<<20)

		for {
			if _, err := br.Peek(1); err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			entLen, err := binary.ReadUvarint(br)
			if err != nil {
				return err
			}

			if entLen > uint64(carutil.MaxAllowedSectionSize) {
				return xerrors.New("malformed car; header is bigger than util.MaxAllowedSectionSize")
			}
			if entLen > uint64(len(entBuf)) {
				entBuf = make([]byte, 1<<bits.Len32(uint32(entLen)))
			}

			_, err = io.ReadFull(br, entBuf[:entLen])
			if err != nil {
				if err == io.ErrUnexpectedEOF {
					return xerrors.Errorf("reading entry, truncated: %w", err)
				} else {
					return xerrors.Errorf("reading entry: %w", err)
				}
			}

			_, c, err := cid.CidFromBytes(entBuf[:entLen])
			if err != nil {
				return xerrors.Errorf("parsing cid: %w", err)
			}

			if c.Prefix().Codec != cid.Raw {
				// todo: allow non-raw flag
				return xerrors.Errorf("non-raw cid %s", c)
			}

			res, err := li.Get([]multihash.Multihash{c.Hash()})
			if err != nil {
				return xerrors.Errorf("get: %w", err)
			}
			if res[0] == -1 {
				notIndexedSet.Add(c)
			} else {
				seenSet.Add(c)
			}

			// Update the progress bar
			bar.Add(int(entLen))
		}

		notCarlogSet := cid.NewSet()

		err = li.List(func(c multihash.Multihash, offs []int64) error {
			if !seenSet.Has(cid.NewCidV1(cid.Raw, c)) {
				notCarlogSet.Add(cid.NewCidV1(cid.Raw, c))
			}
			return nil
		})
		if err != nil {
			return xerrors.Errorf("list: %w", err)
		}

		for _, c := range notCarlogSet.Keys() {
			fmt.Println("indexed not in log:", c.String())
		}

		for _, c := range notIndexedSet.Keys() {
			fmt.Println("in log not indexed:", c.String())
		}

		fmt.Printf("indexed: %d, not indexed: %d, not in log: %d\n", seenSet.Len(), notIndexedSet.Len(), notCarlogSet.Len())

		return nil
	},
}

var checkOffsetsCmd = &cli.Command{
	Name:      "check-offsets",
	Usage:     "checks that the index offsets match entry offsets in the carlog and provides aggregate statistics",
	ArgsUsage: "[carlog file] [leveldb file]",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "repair",
			Usage: "repair incorrect offsets",
		},
	},
	Action: func(c *cli.Context) error {
		if c.NArg() != 2 {
			return cli.Exit("Invalid number of arguments", 1)
		}

		li, err := carlog.OpenLevelDBIndex(c.Args().Get(1), false)
		if err != nil {
			return xerrors.Errorf("open leveldb index: %w", err)
		}

		defer func() {
			if err := li.Close(); err != nil {
				panic(err)
			}
		}()

		carlogFile, err := os.Open(c.Args().First())
		if err != nil {
			return xerrors.Errorf("open carlog file: %w", err)
		}
		defer carlogFile.Close()

		fileInfo, err := carlogFile.Stat()
		if err != nil {
			return xerrors.Errorf("retrieving file info: %w", err)
		}

		br := bufio.NewReader(carlogFile)

		// Read the CAR header
		ch, err := car.ReadHeader(br)
		if err != nil {
			return xerrors.Errorf("reading car header: %w", err)
		}

		bar := pb.New64(int64(fileInfo.Size())).Start()
		bar.Units = pb.U_BYTES

		hlen, err := car.HeaderSize(ch)
		if err != nil {
			return xerrors.Errorf("calculating car header size: %w", err)
		}
		bar.Add(int(hlen))

		entBuf := make([]byte, 16<<20)
		var currOffset int64 = int64(hlen)
		var matchedOffsets, mismatchedOffsets int64 = 0, 0

		repair := c.Bool("repair")

		for {
			if _, err := br.Peek(1); err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			entLen, err := binary.ReadUvarint(br)
			if err != nil {
				return err
			}

			if entLen > uint64(carutil.MaxAllowedSectionSize) {
				return xerrors.New("malformed car; header is bigger than util.MaxAllowedSectionSize")
			}
			if entLen > uint64(len(entBuf)) {
				entBuf = make([]byte, 1<<bits.Len32(uint32(entLen)))
			}

			_, err = io.ReadFull(br, entBuf[:entLen])
			if err != nil {
				return xerrors.Errorf("reading entry: %w", err)
			}

			_, c, err := cid.CidFromBytes(entBuf[:entLen])
			if err != nil {
				return xerrors.Errorf("parsing cid: %w", err)
			}

			res, err := li.Get([]multihash.Multihash{c.Hash()})
			if err != nil {
				return xerrors.Errorf("get: %w", err)
			}

			idxOffset, elen := fromOffsetLen(res[0])
			if idxOffset != currOffset || elen != int(entLen) {
				if repair {
					err := li.Put([]multihash.Multihash{c.Hash()}, []int64{makeOffsetLen(currOffset, int(entLen))})
					if err != nil {
						return xerrors.Errorf("repairing offset: %w", err)
					}
				}
				mismatchedOffsets++
			} else {
				matchedOffsets++
			}

			// Move the current offset forward
			l := int64(binary.PutUvarint(entBuf, entLen)) + int64(entLen)
			bar.Add(int(l))

			currOffset += l
		}

		fmt.Printf("%d offsets match, %d offsets mismatched\n", matchedOffsets, mismatchedOffsets)
		return nil
	},
}

func fromOffsetLen(offlen int64) (int64, int) {
	return offlen & 0xFFFF_FFFF_FF, int(offlen >> 40)
}

func makeOffsetLen(off int64, length int) int64 {
	return (int64(length) << 40) | (off & 0xFFFF_FFFF_FF)
}
