package main

import (
	"fmt"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon"

	cli "github.com/jawher/mow.cli"
	"golang.org/x/net/context"
)

func ListModules(cmd *cli.Cmd) {
	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		request := new(pb.Empty)

		ctx := context.Background()
		reply, err := client.ListModules(ctx, request)
		if err != nil {
			panic(err)
		}

		fmt.Println(reply)
	}
}

func ListSessions(cmd *cli.Cmd) {
	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		request := new(pb.Empty)

		ctx := context.Background()
		reply, err := client.ListSessions(ctx, request)
		if err != nil {
			panic(err)
		}

		fmt.Println(reply)
	}
}
