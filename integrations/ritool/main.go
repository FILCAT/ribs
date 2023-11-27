package main

import (
	"github.com/urfave/cli/v2"
	"os"
)

func main() {
	app := cli.App{
		Name:  "ribs",
		Usage: "ribs repository manipulation commands",

		Commands: []*cli.Command{
			headCmd,
			carlogCmd,
			idxLevelCmd,
			ldbcidCmd,
		},
	}

	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
}
