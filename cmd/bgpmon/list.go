package main

import (
	"fmt"

	pb "github.com/CSUNetSec/bgpmon/protobuf"

	cli "github.com/jawher/mow.cli"
	"golang.org/x/net/context"
)

func ListModules(cmd *cli.Cmd) {
	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		config := new(pb.Empty)

		ctx := context.Background()
		res, err := client.ListModules(ctx, config)
		if err != nil {
			panic(err)
		}

		fmt.Println(res)
	}
}

func ListSessions(cmd *cli.Cmd) {
	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		config := new(pb.Empty)

		ctx := context.Background()
		res, err := client.ListSessions(ctx, config)
		if err != nil {
			panic(err)
		}

		fmt.Println(res)
	}
}
