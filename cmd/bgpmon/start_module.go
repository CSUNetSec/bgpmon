package main

import (
	"fmt"

	cli "github.com/jawher/mow.cli"
)

func StartGoBGPDLinkModule(cmd *cli.Cmd) {
	cmd.Spec = "HOST"
	host := cmd.StringArg("HOST", "", "host of gobgpd instance")

	cmd.Action = func() {
		fmt.Println("TODO initiate connection with gobgpd instance at host:", *host)
	}
}

func StartPrefixHijackModule(cmd *cli.Cmd) {
	cmd.Spec = "PREFIX"
	prefix := cmd.StringArg("PREFIX", "", "prefix to monitor")

	cmd.Action = func() {
		fmt.Println("TODO start prefix hijack module on prefix:", *prefix)
	}
}
