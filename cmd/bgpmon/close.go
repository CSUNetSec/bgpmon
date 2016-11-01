package main

import (
	"fmt"

	pb "github.com/CSUNetSec/bgpmon/pb"

	cli "github.com/jawher/mow.cli"
	"golang.org/x/net/context"
)

func Close(cmd *cli.Cmd) {
	cmd.Spec = "ID"
	id := cmd.StringArg("ID", "", "id of bgpmond session")

	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		request := &pb.CloseSessionRequest{*id}

		ctx := context.Background()
		reply, err := client.CloseSession(ctx, request)
		if err != nil {
			panic(err)
		}

		fmt.Println(reply)
	}
}
