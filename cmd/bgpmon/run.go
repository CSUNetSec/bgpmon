package main

import (
	"fmt"
	"strings"
	"time"

	pbbgpmon "github.com/CSUNetSec/netsec-protobufs/bgpmon"

	cli "github.com/jawher/mow.cli"
	"golang.org/x/net/context"
)

func RunPrefixByAsNumberModule(cmd *cli.Cmd) {
	cmd.Spec = "START_TIME END_TIME SESSION_IDS"
	startTime := cmd.StringArg("START_TIME", "1990-05-28 00:00:00+0000", "Start time for analysis")
	endTime := cmd.StringArg("END_TIME", "1990-05-28 23:59:59+0000", "Start time for analysis")
	inSessions := cmd.StringArg("SESSION_IDS", "", "comma separated list of session ids to use as input")

	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		startTimeParsed, err := time.Parse("2006-01-02 15:04:05-0700", *startTime)
		if err != nil {
			panic(err)
		}

		endTimeParsed, err := time.Parse("2006-01-02 15:04:05-0700", *endTime)
		if err != nil {
			panic(err)
		}

		prefixByAsNumber := pbbgpmon.PrefixByAsNumberModule{
			StartTime:   int64(startTimeParsed.Unix()),
			EndTime:     int64(endTimeParsed.Unix()),
			InSessionId: strings.Split(*inSessions, ","),
		}

		request := new(pbbgpmon.RunModuleRequest)
		request.Type = pbbgpmon.ModuleType_PREFIX_BY_AS_NUMBER
		request.PrefixByAsNumberModule = &prefixByAsNumber

		ctx := context.Background()
		reply, err := client.RunModule(ctx, request)
		if err != nil {
			panic(err)
		}

		fmt.Println(reply)
	}
}

func RunPrefixHijackModule(cmd *cli.Cmd) {
    cmd.Spec = ""
    timeoutSeconds := cmd.IntOpt("timeout_secs", 60, "stop module execution if time exeeds this limit")
	inSessions := cmd.StringArg("SESSION_IDS", "", "comma separated list of session ids to use as input")

    cmd.Action = func() {
        client, err := getRPCClient()
        if err != nil {
            panic(err)
        }

        //create request
		prefixHijack := pbbgpmon.PrefixHijackModule{
			PeriodicSeconds: 0,
			TimeoutSeconds:  int32(*timeoutSeconds),
			InSessionId:     strings.Split(*inSessions, ","),
		}

		request := new(pbbgpmon.RunModuleRequest)
		request.Type = pbbgpmon.ModuleType_PREFIX_HIJACK
		request.PrefixHijackModule = &prefixHijack

        //send request
		ctx := context.Background()
		reply, err := client.RunModule(ctx, request)
		if err != nil {
			panic(err)
		}

        fmt.Println(reply)
    }
}
