package main

import (
	"fmt"
	"strconv"
	"strings"

	pb "github.com/CSUNetSec/bgpmon/protobuf"

	cli "github.com/jawher/mow.cli"
	"golang.org/x/net/context"
)

func StartGoBGPDLinkModule(cmd *cli.Cmd) {
	cmd.Spec = "ADDRESS SESSION_IDS"
	address := cmd.StringArg("ADDRESS", "", "address of gobgpd instance")
	outSessions := cmd.StringArg("SESSION_IDS", "", "comma separated list of session ids use as output")

	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		config := new(pb.StartModuleConfig)
		config.Type = pb.ModuleType_GOBGP_LINK
		config.GobgpLinkModule = &pb.GoBGPLinkModule{*address, strings.Split(*outSessions, ",")}

		ctx := context.Background()
		res, err := client.StartModule(ctx, config)
		if err != nil {
			panic(err)
		}

		fmt.Println(res)
	}
}

func StartPrefixHijackModule(cmd *cli.Cmd) {
	cmd.Spec = "PREFIX AS_NUMBERS SESSION_IDS [--periodic_secs] [--timeout_secs]"
	prefix := cmd.StringArg("PREFIX", "", "prefix to monitor")
	asNumbers := cmd.StringArg("AS_NUMBERS", "", "comma separated list of valid as numbers that advertise this prefix")
	periodicSeconds := cmd.IntOpt("periodic_secs", 30, "delay between monitoring checks for a prefix hijack")
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
			PeriodicSeconds: uint32(*periodicSeconds),
			TimeoutSeconds:  uint32(*timeoutSeconds),
			InSessionId:     strings.Split(*inSessions, ","),
		}

		config := new(pb.StartModuleConfig)
		config.Type = pb.ModuleType_PREFIX_HIJACK
		config.PrefixHijackModule = &prefixHijack

		ctx := context.Background()
		res, err := client.StartModule(ctx, config)
		if err != nil {
			panic(err)
		}

		fmt.Println(res)
	}
}
