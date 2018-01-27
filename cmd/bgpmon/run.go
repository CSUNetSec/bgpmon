package main

import (
	"fmt"
	"strings"
	"time"

	pbbgpmon "github.com/CSUNetSec/netsec-protobufs/bgpmon"

	cli "github.com/jawher/mow.cli"
	"golang.org/x/net/context"
	"strconv"
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
	startDateStr := cmd.StringArg("start_date", "dontparse", "date string in format 2006-Jan-02. Times will be UTC")
	loobackdays := cmd.IntOpt("lookback days", 7, "number of days to look back for hijacks")
	const shortForm = "2006-Jan-02"
	t, err := time.Parse(shortForm, *startDateStr)
	if err != nil {
		panic(err)
	}
	secsfromepoch := t.Unix()

	cmd.Action = func() {
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		//create request
		prefixHijack := pbbgpmon.PrefixHijackModule{
			PeriodicSeconds:           0,
			TimeoutSeconds:            int32(*timeoutSeconds),
			InSessionId:               strings.Split(*inSessions, ","),
			LookbackDurationSeconds:   int64(*loobackdays * 24 * 3600),
			StartTimeSecondsFromEpoch: secsfromepoch,
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

func RunLookingGlassModule(cmd *cli.Cmd) {
	const shortForm = "2006-Jan-02"
	cmd.Spec = "SESSION_IDS START_DATE END_DATE"
	sessions := cmd.StringArg("SESSION_IDS START_DATE END_DATE", "", "comman separated list of session ids to be used")
	startDateStr := cmd.StringArg("START_DATE", "2018-01-01 00:00:00+0000", "start date")
	endDateStr := cmd.StringArg("END_DATE", "2018-01-02 00:00:00+0000", "end date")
	asns := cmd.StringOpt("asns", "", "list of AS numbers to get prefixes for")
	prefixes := cmd.StringOpt("prefs", "", "list of prefixes to get ASNs for")
	peers := cmd.StringOpt("peers", "", "list of peers to narrow down the results")
	t1, err1 := time.Parse(shortForm, *startDateStr)
	t2, err2 := time.Parse(shortForm, *endDateStr)
	if err1 != nil || err2 != nil {
		panic(fmt.Sprintf("err start time:%s err end time:%s", err1, err2))
	}
	t1s, t2s := t1.Unix(), t2.Unix()
	asnslice := strings.Split(*asns, ",")
	asnsliceInts := make([]uint32, len(asnslice))
	for i, v := range asnslice {
		if num, err := strconv.ParseUint(v, 10, 32); err != nil {
			asnsliceInts[i] = uint32(num)
		} else {
			panic(err)
		}
	}
	prefslice := strings.Split(*prefixes, ",")
	peerslice := strings.Split(*peers, ",")
	sessionslice := strings.Split(*sessions, ",")
	cmd.Action = func() {
		cli, err := getRPCClient()
		if err != nil {
			panic(err)
		}
		lookingGlass := pbbgpmon.LookingGlassModule{
			StartTime: t1s,
			EndTime:   t2s,
			Prefixes:  prefslice,
			Asns:      asnsliceInts,
			Peers:     peerslice,
			SessionId: sessionslice,
		}
		request := new(pbbgpmon.RunModuleRequest)
		request.Type = pbbgpmon.ModuleType_LOOKING_GLASS
		request.LookingGlassModule = &lookingGlass

		//send request
		ctx := context.Background()
		reply, err := cli.RunModule(ctx, request)
		if err != nil {
			panic(err)
		}
		fmt.Println(reply)

	}
}
