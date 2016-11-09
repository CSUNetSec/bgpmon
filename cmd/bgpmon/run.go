package main

import (
    "bufio"
	"fmt"
    "net"
    "os"
	"strconv"
	"strings"
	"time"

	pbcommon "github.com/CSUNetSec/netsec-protobufs/common"
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
    file := cmd.StringArg("FILE", "", "csv file formatted as 'AS_NUMBER,IP_ADDRESS,MASK'")
    timeoutSeconds := cmd.IntOpt("timeout_secs", 60, "stop module execution if time exeeds this limit")
	inSessions := cmd.StringArg("SESSION_IDS", "", "comma separated list of session ids to use as input")

    cmd.Action = func() {
        client, err := getRPCClient()
        if err != nil {
            panic(err)
        }

        //parse monitor prefixes
        file, err := os.Open(*file)
        if err != nil {
            panic(err)
        }
        defer file.Close()

        scanner := bufio.NewScanner(file)
        monitorPrefixes := []*pbbgpmon.PrefixHijackModule_MonitorPrefix{}
        for scanner.Scan() {
            if scanner.Err() != nil {
                break
            }

            //parse fields
            fields := strings.Split(scanner.Text(), ",")

            asNumber, err := strconv.ParseInt(fields[0], 0, 32)
            if err != nil {
                panic(err)
            }

            ipAddress := net.ParseIP(fields[1])

            mask, err := strconv.ParseInt(fields[2], 0, 32)
            if err != nil {
                panic(err)
            }

            //check if montior prefix already exists
            found := false
            for _, monitorPrefix := range monitorPrefixes {
                if monitorPrefix.Prefix.Mask != uint32(mask) {
                    continue
                }

                if !compareIpNet(net.IP(monitorPrefix.Prefix.Prefix.Ipv4), ipAddress) {
                    continue
                }

                monitorPrefix.AsNumber = append(monitorPrefix.AsNumber, uint32(asNumber))
                found = true
                break
            }

            if found {
                continue
            }

            //create monitor prefix protobuf
            prefixWrapper := pbcommon.PrefixWrapper {
                Prefix: &pbcommon.IPAddressWrapper { Ipv4: ipAddress },
                Mask: uint32(mask),
            }

            monitorPrefix := pbbgpmon.PrefixHijackModule_MonitorPrefix {
                Prefix: &prefixWrapper,
                AsNumber: []uint32{uint32(asNumber)},
            }

            monitorPrefixes = append(monitorPrefixes, &monitorPrefix)
        }

        //create request
		prefixHijack := pbbgpmon.PrefixHijackModule{
            MonitorPrefixes: monitorPrefixes,
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
