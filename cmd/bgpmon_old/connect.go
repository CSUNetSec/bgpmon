package main

import (
	pb "bgpmon/proto/bgpmond"
	"fmt"
	"github.com/codegangsta/cli"
	"golang.org/x/net/context"
)

func connCassandra(c *cli.Context) {
	args := c.Args()
	conn, err := connectRPC()
	if err != nil {
		panic(err)
	}

	client := pb.NewBgpmondClient(conn)

	ctx := context.Background()
	cconf := &pb.ConnectConf{}
	cconf.Type = pb.ConnectConf_CASSANDRA
	cconf.Username = args[0]
	cconf.Password = args[1]
	cconf.Host = args[2:]

	res, err := client.Connect(ctx, cconf)
	if err != nil {
		panic(err)
	}

	fmt.Println(res)
}

func connFile(c *cli.Context) {
	fmt.Println("Connect File unimplemeneted")
}
