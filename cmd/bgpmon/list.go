package main

import (
	"fmt"

	cli "github.com/jawher/mow.cli"
)

func ListModules(cmd *cli.Cmd) {
	cmd.Action = func() {
		fmt.Println("TODO listing modules")
	}
}

func ListSessions(cmd *cli.Cmd) {
	cmd.Action = func() {
		fmt.Println("TODO listing sessions")
	}
}
