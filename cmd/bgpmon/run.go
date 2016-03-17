package main

import (
	"fmt"
	"strconv"
	"strings"

	pb "github.com/CSUNetSec/bgpmon/protobuf"

	cli "github.com/jawher/mow.cli"
	"golang.org/x/net/context"
)

func RunPrefixHijackModule(cmd *cli.Cmd) {
	cmd.Spec = "PREFIX AS_NUMBERS SESSION_IDS [--timeout_secs]"
	prefix := cmd.StringArg("PREFIX", "", "prefix to monitor")
	asNumbers := cmd.StringArg("AS_NUMBERS", "", "comma separated list of valid as numbers that advertise this prefix")
	timeoutSeconds := cmd.IntOpt("timeout_secs", 60, "stop module execution if time exceeds this limit")
	inSessions := cmd.StringArg("SESSION_IDS", "", "comma separated list of session ids to use as input")

	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		asNums := []uint32{}
		for _, asNumber := range strings.Split(*asNumbers, ",") {
			asNum, err := strconv.ParseUint(asNumber, 0, 32)
			if err != nil {
				panic(err)
			}
			asNums = append(asNums, uint32(asNum))
		}

		prefixHijack := pb.PrefixHijackModule{
			Prefix:          *prefix,
			AsNumber:        asNums,
			PeriodicSeconds: 0,
			TimeoutSeconds:  int32(*timeoutSeconds),
			InSessionId:     strings.Split(*inSessions, ","),
		}

		request := new(pb.StartModuleRequest)
		request.Type = pb.ModuleType_PREFIX_HIJACK
		request.PrefixHijackModule = &prefixHijack

		ctx := context.Background()
		reply, err := client.StartModule(ctx, request)
		if err != nil {
			panic(err)
		}

		fmt.Println(reply)
	}
}
