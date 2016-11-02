package main

import (
	"fmt"
	"strconv"
	"strings"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon"

	cli "github.com/jawher/mow.cli"
	"golang.org/x/net/context"
)

func StartGoBGPDLinkModule(cmd *cli.Cmd) {
	cmd.Spec = "ADDRESS SESSION_IDS [--module_id]"
	addreplys := cmd.StringArg("ADDRESS", "", "addreplys of gobgpd instance")
	outSessions := cmd.StringArg("SESSION_IDS", "", "comma separated list of session ids use as output")
	moduleID := cmd.StringOpt("module_id", getUUID(), "id of new module")

	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		request := new(pb.StartModuleRequest)
		request.Type = pb.ModuleType_GOBGP_LINK
		request.ModuleId = *moduleID
		request.GobgpLinkModule = &pb.GoBGPLinkModule{*addreplys, strings.Split(*outSessions, ",")}

		ctx := context.Background()
		reply, err := client.StartModule(ctx, request)
		if err != nil {
			panic(err)
		}

		fmt.Println(reply)
	}
}

func StartPrefixHijackModule(cmd *cli.Cmd) {
	cmd.Spec = "PREFIX AS_NUMBERS SESSION_IDS [--module_id] [--periodic_secs] [--timeout_secs]"
	prefix := cmd.StringArg("PREFIX", "", "prefix to monitor")
	asNumbers := cmd.StringArg("AS_NUMBERS", "", "comma separated list of valid as numbers that advertise this prefix")
	inSessions := cmd.StringArg("SESSION_IDS", "", "comma separated list of session ids to use as input")
	moduleID := cmd.StringOpt("module_id", getUUID(), "id of new module")
	periodicSeconds := cmd.IntOpt("periodic_secs", 30, "delay between monitoring checks for a prefix hijack")
	timeoutSeconds := cmd.IntOpt("timeout_secs", 60, "stop module execution if time exceeds this limit")

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
			PeriodicSeconds: int32(*periodicSeconds),
			TimeoutSeconds:  int32(*timeoutSeconds),
			InSessionId:     strings.Split(*inSessions, ","),
		}

		request := new(pb.StartModuleRequest)
		request.Type = pb.ModuleType_PREFIX_HIJACK
		request.ModuleId = *moduleID
		request.PrefixHijackModule = &prefixHijack

		ctx := context.Background()
		reply, err := client.StartModule(ctx, request)
		if err != nil {
			panic(err)
		}

		fmt.Println(reply)
	}
}
