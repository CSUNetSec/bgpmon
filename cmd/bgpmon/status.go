package main

import (
	"fmt"

	cli "github.com/jawher/mow.cli"
)

func StatusModule(cmd *cli.Cmd) {
	cmd.Spec = "ID"
	id := cmd.StringArg("ID", "", "id of bgpmond module")

	cmd.Action = func() {
		fmt.Println("TODO checking status of module with id:", id)
	}
}

func StatusSession(cmd *cli.Cmd) {
	cmd.Spec = "ID"
	id := cmd.StringArg("ID", "", "id of bgpmond session")

	cmd.Action = func() {
		fmt.Println("TODO checking status of session with id:", id)
	}
}
