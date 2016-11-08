package main

import (
    "bufio"
	"fmt"
    "net"
    "os"
    "strconv"
	"strings"

	pbcommon "github.com/CSUNetSec/netsec-protobufs/common"
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
    file := cmd.StringArg("FILE", "", "csv file formatted as 'AS_NUMBER,IP_ADDRESS,MASK'")
	inSessions := cmd.StringArg("SESSION_IDS", "", "comma separated list of session ids to use as input")
	moduleID := cmd.StringOpt("module_id", getUUID(), "id of new module")
	periodicSeconds := cmd.IntOpt("periodic_secs", 30, "delay between monitoring checks for a prefix hijack")
	timeoutSeconds := cmd.IntOpt("timeout_secs", 60, "stop module execution if time exceeds this limit")

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
