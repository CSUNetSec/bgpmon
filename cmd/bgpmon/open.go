package main

import (
	"fmt"
	"strings"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon"

	cli "github.com/jawher/mow.cli"
	"golang.org/x/net/context"
)

func OpenCassandra(cmd *cli.Cmd) {
	cmd.Spec = "USERNAME PASSWORD HOSTS [--session_id] [--worker_count]"
	username := cmd.StringArg("USERNAME", "", "username for cassandra connection")
	password := cmd.StringArg("PASSWORD", "", "password for cassandra connection")
	hosts := cmd.StringArg("HOSTS", "", "list of cassandra hosts")
	sessionID := cmd.StringOpt("session_id", getUUID(), "id of the session")
	workerCount := cmd.IntOpt("worker_count", 20000, "size of the data writing worker pool")

	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		request := new(pb.OpenSessionRequest)
		request.Type = pb.SessionType_CASSANDRA
		request.SessionId = *sessionID
		fmt.Printf("HOSTS string is %s USERNAME is %s\n", *hosts, *username)
		request.CassandraSession = &pb.CassandraSession{*username, *password, strings.Split(*hosts, ","), uint32(*workerCount)}

		ctx := context.Background()
		reply, err := client.OpenSession(ctx, request)
		if err != nil {
			panic(err)
		}

		fmt.Println(reply)
	}
}

func OpenCockroach(cmd *cli.Cmd) {
    cmd.Spec = "USERNAME CERTDIR HOSTS [--session_id] [--port] [--worker_count]"
	username := cmd.StringArg("USERNAME", "", "username for cockroach connection")
	certdir := cmd.StringArg("CERTDIR", "", "directory that contains ssl certs")
	hosts := cmd.StringArg("HOSTS", "", "list of cockroach hosts")
	sessionID := cmd.StringOpt("session_id", getUUID(), "id of the session")
    portNumber := cmd.IntOpt("port", 26257, "port where service is running on host(s)")
	workerCount := cmd.IntOpt("worker_count", 40, "size of the data writing worker pool")
	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		request := new(pb.OpenSessionRequest)
		request.Type = pb.SessionType_COCKROACH
		request.SessionId = *sessionID
		fmt.Printf("HOSTS string is %s USERNAME is %s\n", *hosts, *username)
        request.CockroachSession = &pb.CockroachSession{Username: *username, Hosts: strings.Split(*hosts, ","), Port: uint32(*portNumber), WorkerCount: uint32(*workerCount), Certdir: *certdir}

		ctx := context.Background()
		reply, err := client.OpenSession(ctx, request)
		if err != nil {
			panic(err)
		}

		fmt.Println(reply)
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

		request := new(pb.OpenSessionRequest)
		request.Type = pb.SessionType_FILE
		request.SessionId = *sessionID
		request.FileSession = &pb.FileSession{*filename}

		ctx := context.Background()
		reply, err := client.OpenSession(ctx, request)
		if err != nil {
			panic(err)
		}

		fmt.Println(reply)
	}
}
