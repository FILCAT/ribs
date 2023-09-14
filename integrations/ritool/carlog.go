package main

import (
	"bufio"
	"fmt"
	"io"
	"math/bits"
	"os"
	"time"

	"encoding/binary"
	"github.com/cheggaaa/pb"
	"github.com/fatih/color"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var carlogCmd = &cli.Command{
	Name:  "carlog",
	Usage: "Carlog commands",
	Subcommands: []*cli.Command{
		carlogAnalyseCmd,
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
			lastByteOffset += int64(binary.PutUvarint(entBuf, entLen)) + int64(entLen)

			// Update the progress bar
			bar.Add(int(entLen))
		}

		// Finish the progress bar
		bar.Finish()

		// Output results
		fmt.Println("Car Header (Root CID):", header.Roots[0])
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

// Get carlog cids

// Recreate level index
