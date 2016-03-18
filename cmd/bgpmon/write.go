package main

import (
	"fmt"

	cli "github.com/jawher/mow.cli"
)

func WriteMRTFile(cmd *cli.Cmd) {
	cmd.Spec = "FILENAME SESSION_ID"
	filename := cmd.StringArg("FILENAME", "", "filename of mrt file")
	sessionID := cmd.StringArg("SESSION_ID", "", "session to write data")

	cmd.Action = func() {
		fmt.Printf("TODO write bgp messages from mrt file %s to session %s\n", *filename, *sessionID)
	}
}

func WriteASLocationFile(cmd *cli.Cmd) {
	cmd.Spec = "FILENAME SESSION_ID"
	filename := cmd.StringArg("FILENAME", "", "filename of as number location file")
	sessionID := cmd.StringArg("SESSION_ID", "", "session to write data")

	cmd.Action = func() {
		fmt.Printf("TODO write as number locations from file %s to session %s\n", *filename, *sessionID)
	}
}
