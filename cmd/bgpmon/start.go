package main

import (
	"fmt"
	"strings"

	pbbgpmon "github.com/CSUNetSec/netsec-protobufs/bgpmon"

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

		request := new(pbbgpmon.StartModuleRequest)
		request.Type = pbbgpmon.ModuleType_GOBGP_LINK
		request.ModuleId = *moduleID
		request.GobgpLinkModule = &pbbgpmon.GoBGPLinkModule{*addreplys, strings.Split(*outSessions, ",")}

		ctx := context.Background()
		reply, err := client.StartModule(ctx, request)
		if err != nil {
			panic(err)
		}

		fmt.Println(reply)
	}
}

func StartPrefixHijackModule(cmd *cli.Cmd) {
	cmd.Spec = ""
	moduleID := cmd.StringArg("MODULE_ID", "", "id of new module")
	inSessions := cmd.StringArg("SESSION_IDS", "", "comma separated list of session ids to use as input")
	periodicSeconds := cmd.IntOpt("periodic_secs", 3600, "delay between monitoring checks for a prefix hijack")
	timeoutSeconds := cmd.IntOpt("timeout_secs", 3600, "stop module execution if time exceeds this limit")

	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		//create request
		prefixHijack := pbbgpmon.PrefixHijackModule{
			PeriodicSeconds: int32(*periodicSeconds),
			TimeoutSeconds:  int32(*timeoutSeconds),
			InSessionId:     strings.Split(*inSessions, ","),
		}

		request := new(pbbgpmon.StartModuleRequest)
		request.Type = pbbgpmon.ModuleType_PREFIX_HIJACK
		request.ModuleId = *moduleID
		request.PrefixHijackModule = &prefixHijack

		//send request
		ctx := context.Background()
		reply, err := client.StartModule(ctx, request)
		if err != nil {
			panic(err)
		}

		fmt.Println(reply)
	}
}
