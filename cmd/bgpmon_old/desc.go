package main

import (
	pb "bgpmon/proto/bgpmond"
	"fmt"
	"github.com/codegangsta/cli"
	"golang.org/x/net/context"
)

func descConns(c *cli.Context) {
	fmt.Println("Describe Connections unimplemeneted")
}

func descModules(c *cli.Context) {
	conn, err := connectRPC()
	if err != nil {
		panic(err)
	}

	client := pb.NewBgpmondClient(conn)
	ctx := context.Background()
	e := &pb.Empty{}
	res, err := client.ModuleList(ctx, e)
	if err != nil {
		panic(err)
	}

	fmt.Println("Modules available:")
	for _, module := range res.ModuleNames {
		fmt.Println("\t", module)
	}
}
