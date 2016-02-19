package main

import (
	pb "bgpmon/proto/bgpmond"
	"fmt"
	"github.com/codegangsta/cli"
	"golang.org/x/net/context"
	"io/ioutil"
)

func startModule(c *cli.Context) {
	args := c.Args()
	conn, err := connectRPC()
	if err != nil {
		panic(err)
	}

	client := pb.NewBgpmondClient(conn)

	confbytes, err := ioutil.ReadFile(args[1])
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	conf := &pb.ModuleStartConf{}
	conf.ModuleName = args[0]
	conf.ModuleConfig = string(confbytes)
	conf.ModuleInSessions = []string{args[2]}
	conf.ModuleOutSessions = []string{args[3]}
	res, err := client.ModuleStart(ctx, conf)
	if err != nil {
		panic(err)
	}

	fmt.Println(res)
}
