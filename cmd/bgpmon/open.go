package main

import (
	"fmt"
	"strings"

	pb "github.com/CSUNetSec/bgpmon/protobuf"

	cli "github.com/jawher/mow.cli"
	"golang.org/x/net/context"
)

func OpenCassandra(cmd *cli.Cmd) {
	cmd.Spec = "USERNAME PASSWORD HOSTS [--session_id]"
	username := cmd.StringArg("USERNAME", "", "username for cassandra connection")
	password := cmd.StringArg("PASSWORD", "", "password for cassandra connection")
	hosts := cmd.StringArg("HOSTS", "", "comma separated list of cassandra hosts")
	sessionID := cmd.StringOpt("session_id", getUUID(), "id of the session")

	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		config := new(pb.OpenSessionConfig)
		config.Type = pb.SessionType_CASSANDRA
		config.SessionId = *sessionID
		config.CassandraSession = &pb.CassandraSession{*username, *password, strings.Split(*hosts, ",")}

		ctx := context.Background()
		res, err := client.OpenSession(ctx, config)
		if err != nil {
			panic(err)
		}

		fmt.Println(res)
	}
}

func OpenFile(cmd *cli.Cmd) {
	cmd.Spec = "FILENAME [--session_id]"
	filename := cmd.StringArg("FILENAME", "", "filename of session file")
	sessionID := cmd.StringOpt("session_id", getUUID(), "id of the session")

	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		config := new(pb.OpenSessionConfig)
		config.Type = pb.SessionType_FILE
		config.SessionId = *sessionID
		config.FileSession = &pb.FileSession{*filename}

		ctx := context.Background()
		res, err := client.OpenSession(ctx, config)
		if err != nil {
			panic(err)
		}

		fmt.Println(res)
	}
}
