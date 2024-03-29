package main

import (
	"bufio"
	"fmt"
	"github.com/multiformats/go-multicodec"
	"io"
	"math/bits"
	"os"
	"strconv"
	"time"

	"encoding/binary"
	"github.com/cheggaaa/pb"
	"github.com/fatih/color"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var carlogCmd = &cli.Command{
	Name:  "carlog",
	Usage: "Carlog commands",
	Subcommands: []*cli.Command{
		carlogAnalyseCmd,
		carlogBottomBoundsCmd,
		readCarEntryCmd,
	},
}

var carlogAnalyseCmd = &cli.Command{
	Name:      "analyse",
	Usage:     "Analyse a carlog file",
	ArgsUsage: "[carlog file]",
	Action: func(c *cli.Context) error {
		if c.NArg() != 1 {
			return cli.Exit("Invalid number of arguments", 1)
		}

		carlogFile, err := os.Open(c.Args().First())
		if err != nil {
			return xerrors.Errorf("open carlog file: %w", err)
		}
		defer carlogFile.Close()

		// Initialize progress bar
		fileInfo, err := carlogFile.Stat()
		if err != nil {
			return xerrors.Errorf("retrieving file info: %w", err)
		}
		bar := pb.New64(int64(fileInfo.Size())).Start()
		bar.Units = pb.U_BYTES

		br := bufio.NewReader(carlogFile)

		// Read the CAR header
		header, err := car.ReadHeader(br)
		if err != nil {
			return xerrors.Errorf("reading car header: %w", err)
		}

		headLen, err := car.HeaderSize(header)
		if err != nil {
			return xerrors.Errorf("calculating car header size: %w", err)
		}

		var lastLength uint64
		var lastByteOffset = int64(headLen)
		var lastOffset = lastByteOffset
		var isTruncated bool
		var lastCID cid.Cid

		var lastValidCID cid.Cid
		var lastValidOffset int64
		var lastValidLength uint64
		var lastValidByteOffset int64

		entBuf := make([]byte, 16<<20)

		// Record start time
		startTime := time.Now()

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

			lastLength = entLen
			if entLen > uint64(carutil.MaxAllowedSectionSize) {
				return xerrors.New("malformed car; header is bigger than util.MaxAllowedSectionSize")
			}
			if entLen > uint64(len(entBuf)) {
				entBuf = make([]byte, 1<<bits.Len32(uint32(entLen)))
			}

			_, err = io.ReadFull(br, entBuf[:entLen])
			if err != nil {
				if err == io.ErrUnexpectedEOF {
					_, lastCID, err = cid.CidFromBytes(entBuf[:entLen])
					if err != nil {
						fmt.Println("last cid parse error:", err)
					}

					isTruncated = true
					break
				} else {
					return xerrors.Errorf("reading entry: %w", err)
				}
			}

			_, lastCID, err = cid.CidFromBytes(entBuf[:entLen])
			if err != nil {
				return xerrors.Errorf("parsing cid: %w", err)
			}

			if !isTruncated {
				lastValidCID = lastCID
				lastValidOffset = lastOffset
				lastValidLength = lastLength
				lastValidByteOffset = lastByteOffset
			}

			lastOffset = lastByteOffset
			carEntLen := int64(binary.PutUvarint(entBuf, entLen)) + int64(entLen)
			lastByteOffset += carEntLen

			// Update the progress bar
			bar.Add(int(carEntLen))
		}

		// Finish the progress bar
		bar.Finish()

		// Output results
		fmt.Println("Car Header (Root CID):", header.Roots[0])
		fmt.Println("Car Header Length:", headLen)
		fmt.Println("Last Object CID:", lastCID)
		fmt.Println("Last Object Length:", lastLength)
		fmt.Println("Offset of the Last Object:", lastOffset)
		fmt.Println("Offset of the Last Byte of the Last Object:", lastByteOffset)
		fmt.Println("File size:", fileInfo.Size(), "; Diff vs last byte offset:", fileInfo.Size()-lastByteOffset)
		if isTruncated {
			color.Red("The last object is truncated.")
		} else {
			color.Green("The last object is not truncated.")
		}

		fmt.Println()

		fmt.Println("last non-truncated object (will be the same as above in non-truncated car):")

		fmt.Println("CID of the Last Non-Truncated Object:", lastValidCID)
		fmt.Println("First Byte Offset of the Last Non-Truncated Object:", lastValidOffset)
		fmt.Println("Length of the Last Non-Truncated Object:", lastValidLength)
		fmt.Println("Last Byte Offset of the Last Non-Truncated Object:", lastValidByteOffset)

		// Print time taken
		elapsedTime := time.Since(startTime)
		fmt.Println("Time taken for analysis:", elapsedTime)

		return nil
	},
}

var carlogBottomBoundsCmd = &cli.Command{
	Name:      "bottom-bounds",
	Usage:     "find bottom layer byte offset bounds (recover from failed top-tree gen)",
	ArgsUsage: "[carlog file]",
	Action: func(c *cli.Context) error {
		if c.NArg() != 1 {
			return cli.Exit("Invalid number of arguments", 1)
		}

		carlogFile, err := os.Open(c.Args().First())
		if err != nil {
			return xerrors.Errorf("open carlog file: %w", err)
		}
		defer carlogFile.Close()

		// Initialize progress bar
		fileInfo, err := carlogFile.Stat()
		if err != nil {
			return xerrors.Errorf("retrieving file info: %w", err)
		}
		bar := pb.New64(int64(fileInfo.Size())).Start()
		bar.Units = pb.U_BYTES

		br := bufio.NewReader(carlogFile)

		// Read the CAR header
		header, err := car.ReadHeader(br)
		if err != nil {
			return xerrors.Errorf("reading car header: %w", err)
		}

		headLen, err := car.HeaderSize(header)
		if err != nil {
			return xerrors.Errorf("calculating car header size: %w", err)
		}

		bar.Add(int(headLen))

		// iterate and find the last byte offset of the last block with cid.Raw codec
		// carlogs are written with leaf nodes first, so blocks are
		// [raw][raw][raw][raw][raw][cbor][cbor]
		// We want to find the last byte of the last raw block (offset of the first cbor block)

		var lastRawBlockByteOffset int64
		var currentByteOffset = int64(headLen)
		var entBuf = make([]byte, 16<<20)

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

			_, currentCID, err := cid.CidFromBytes(entBuf[:entLen])
			if err != nil {
				return xerrors.Errorf("parsing cid: %w", err)
			}

			// Check if the CID has a Raw codec
			if currentCID.Type() != cid.Raw {
				break
			}

			// Update the last raw block byte offset
			carEntLen := int64(binary.PutUvarint(entBuf, entLen)) + int64(entLen)
			lastRawBlockByteOffset = currentByteOffset + carEntLen
			currentByteOffset = lastRawBlockByteOffset

			// Update the progress bar
			bar.Add(int(carEntLen))
		}

		bar.Finish()

		// Output results
		fmt.Println("Last Byte Offset of the Last Raw Block:", lastRawBlockByteOffset)

		return nil
	},
}

var readCarEntryCmd = &cli.Command{
	Name:      "read-entry",
	Usage:     "Read a single CAR entry from a given offset",
	ArgsUsage: "[carlog file] [offset]",
	Flags: []cli.Flag{
		&cli.Int64Flag{
			Name:  "block-offset",
			Usage: "seek this many blocks before/after the block at the given offset (seek back is expensive)",
		},
	},
	Action: func(c *cli.Context) error {
		if c.NArg() != 2 {
			return cli.Exit("Invalid number of arguments", 1)
		}

		carlogFile, err := os.Open(c.Args().Get(0))
		if err != nil {
			return xerrors.Errorf("open carlog file: %w", err)
		}
		defer carlogFile.Close()

		offset, err := strconv.ParseInt(c.Args().Get(1), 0, 64)
		if err != nil {
			return xerrors.Errorf("parsing offset: %w", err)
		}

		_, err = carlogFile.Seek(offset, 0)
		if err != nil {
			return xerrors.Errorf("seeking to offset: %w", err)
		}

		br := bufio.NewReader(carlogFile)

		entLen, err := binary.ReadUvarint(br)
		if err != nil {
			return err
		}

		if entLen == 0 || entLen > uint64(carutil.MaxAllowedSectionSize) {
			return xerrors.New("invalid entry length from varint header")
		}

		entBuf := make([]byte, carutil.MaxAllowedSectionSize)

		_, err = io.ReadFull(br, entBuf[:entLen])
		if err != nil {
			return xerrors.Errorf("reading entry: %w", err)
		}

		_, currentCID, err := cid.CidFromBytes(entBuf)
		if err != nil {
			return xerrors.Errorf("parsing cid (target offset): %w", err)
		}

		foundOff := offset

		blockOff := c.Int64("block-offset")
		if blockOff > 0 {
			foundOff += int64(entLen) + int64(binary.PutUvarint(entBuf, entLen))

			for i := int64(0); i < blockOff; i++ {
				entLen, err = binary.ReadUvarint(br)
				if err != nil {
					return err
				}

				if i+1 < blockOff {
					foundOff += int64(entLen) + int64(binary.PutUvarint(entBuf, entLen))
				}

				_, err = io.ReadFull(br, entBuf[:entLen])
				if err != nil {
					return xerrors.Errorf("reading entry: %w", err)
				}

				_, currentCID, err = cid.CidFromBytes(entBuf)
				if err != nil {
					return xerrors.Errorf("parsing cid (block +%d): %w", i+1, err)
				}
			}
		} else if blockOff < 0 { // If block offset is negative, start from the beginning and track offsets
			blockOff = -blockOff

			// Seek to the start of the file
			_, err := carlogFile.Seek(0, 0)
			if err != nil {
				return xerrors.Errorf("seek start: %w", err)
			}

			br.Reset(carlogFile)

			// Read the CAR header
			ch, err := car.ReadHeader(br)
			if err != nil {
				return xerrors.Errorf("read header: %w", err)
			}

			hsize, err := car.HeaderSize(ch)
			if err != nil {
				return xerrors.Errorf("calc header size: %w", err)
			}

			// Maintain a list of past offsets with a length equal to the absolute value of blockOff
			offs := make([]int64, blockOff)
			readBlocks := 0

			var at int64
			for at = int64(hsize); at < offset; {
				offs[readBlocks%int(blockOff)] = at

				entLen, err = binary.ReadUvarint(br)
				if err != nil {
					return err
				}

				at += int64(entLen) + int64(binary.PutUvarint(entBuf, entLen))
				readBlocks++

				_, err = io.ReadFull(br, entBuf[:entLen])
				if err != nil {
					return xerrors.Errorf("reading entry: %w", err)
				}
			}

			if at != offset {
				return xerrors.Errorf("block %d is at offset %d and falls past specified offset %d", readBlocks, at, offset)
			}

			// Seek to the desired block after moving back by the specified block offset
			foundOff = offs[(readBlocks-int(blockOff))%int(blockOff)]

			_, err = carlogFile.Seek(foundOff, 0)
			if err != nil {
				return xerrors.Errorf("seek to entry at block offset: %w", err)
			}

			br.Reset(carlogFile)

			// Read the entry at the found offset
			entLen, err = binary.ReadUvarint(br)
			if err != nil {
				return err
			}

			_, err = io.ReadFull(br, entBuf[:entLen])
			if err != nil {
				return xerrors.Errorf("reading entry: %w", err)
			}

			_, currentCID, err = cid.CidFromBytes(entBuf)
			if err != nil {
				return xerrors.Errorf("parsing cid: %w", err)
			}
		}

		// Output results
		fmt.Println("Entry Length:", entLen)
		fmt.Println("Entry Offset:", foundOff)

		fmt.Println("CID:", currentCID)
		fmt.Println("Multihash:", currentCID.Hash())
		fmt.Println("CID Codec (Int):", currentCID.Type())
		fmt.Println("CID Codec (String):", multicodec.Code(currentCID.Type()).String())
		mhash, err := multihash.Decode(currentCID.Hash())
		if err != nil {
			return xerrors.Errorf("decoding multihash: %w", err)
		}
		fmt.Println("Multihash Type (Int):", mhash.Code)
		fmt.Println("Multihash Type (String):", mhash.Name)

		return nil
	},
}

// Recreate level index
