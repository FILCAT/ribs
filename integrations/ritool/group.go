package main

import (
	"fmt"
	"github.com/urfave/cli/v2"
	"strconv"
)

var groupCmd = &cli.Command{
	Name:  "group",
	Usage: "Group utils",
	Subcommands: []*cli.Command{
		groupNumToIdCmd,
	},
}

var groupNumToIdCmd = &cli.Command{
	Name:      "num-to-id",
	Usage:     "Convert group number to group ID",
	ArgsUsage: "[group number]",
	Action: func(c *cli.Context) error {
		if c.NArg() != 1 {
			return cli.Exit("Invalid number of arguments", 1)
		}

		gi, err := strconv.ParseInt(c.Args().First(), 10, 64)
		if err != nil {
			return cli.Exit("Invalid group number", 1)
		}

		fmt.Println(strconv.FormatInt(gi, 32))

		return nil
	},
}
