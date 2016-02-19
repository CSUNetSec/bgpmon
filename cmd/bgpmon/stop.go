package main

import (
	"fmt"

	pb "github.com/hamersaw/bgpmon/proto/bgpmond"

	cli "github.com/jawher/mow.cli"
	"golang.org/x/net/context"
)

func StopModule(cmd *cli.Cmd) {
	cmd.Spec = "ID"
	id := cmd.StringArg("ID", "", "id of bgpmond session")

	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		config := &pb.StopModuleConfig { *id }

		ctx := context.Background()
		res, err := client.StopModule(ctx, config)
		if err != nil {
			panic(err)
		}

		fmt.Println(res)
	}
}
