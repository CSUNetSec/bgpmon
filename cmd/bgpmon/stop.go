package main

import (
	"fmt"

	cli "github.com/jawher/mow.cli"
)

func StopModule(cmd *cli.Cmd) {
	cmd.Spec = "ID"
	id := cmd.StringArg("ID", "", "id of bgpmond module")

	cmd.Action = func() {
		fmt.Println("TODO stopping module with id:", id)
	}
}
