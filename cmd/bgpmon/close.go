package main

import (
	"fmt"

	cli "github.com/jawher/mow.cli"
)

func Close(cmd *cli.Cmd) {
	cmd.Spec = "ID"
	id := cmd.StringArg("ID", "", "id of bgpmond session")

	cmd.Action = func() {
		fmt.Println("TODO disconnecting session with id:", id)
	}
}
