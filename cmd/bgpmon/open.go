package main

import (
	"fmt"
	"strings"

	pb "github.com/hamersaw/bgpmon/protobuf"

	cli "github.com/jawher/mow.cli"
	"golang.org/x/net/context"
)

func OpenCassandra(cmd *cli.Cmd) {
	cmd.Spec = "USERNAME PASSWORD HOSTS"
	username := cmd.StringArg("USERNAME", "", "username for cassandra connection")
	password := cmd.StringArg("PASSWORD", "", "password for cassandra connection")
	hosts := cmd.StringArg("HOSTS", "", "comma separated list of cassandra hosts")

	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		config := new(pb.OpenSessionConfig)
		config.Type = pb.OpenSessionConfig_CASSANDRA
		config.CassandraSession = &pb.CassandraSession { *username, *password, strings.Split(*hosts, ",")}

		ctx := context.Background()
		res, err := client.OpenSession(ctx, config)
		if err != nil {
			panic(err)
		}

		fmt.Println(res)
	}
}

func OpenFile(cmd *cli.Cmd) {
	cmd.Spec = "FILENAME"
	filename := cmd.StringArg("FILENAME", "", "filename of session file")

	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		config := new(pb.OpenSessionConfig)
		config.Type = pb.OpenSessionConfig_FILE
		config.FileSession = &pb.FileSession { *filename }

		ctx := context.Background()
		res, err := client.OpenSession(ctx, config)
		if err != nil {
			panic(err)
		}

		fmt.Println(res)
	}
}
