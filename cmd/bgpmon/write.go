package main

import (
	"fmt"

	cli "github.com/jawher/mow.cli"
)

func WriteMRTFile(cmd *cli.Cmd) {
	cmd.Spec = "FILENAME"
	filename := cmd.StringArg("FILENAME", "", "filename of mrt file")

	cmd.Action = func() {
		fmt.Println("TODO write bgp messages from mrt file with filename:", filename)
	}
}
