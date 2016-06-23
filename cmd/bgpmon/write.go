package main

import (
	"bufio"
    "errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/CSUNetSec/bgpmon/protobuf"

	cli "github.com/jawher/mow.cli"
    gobgp "github.com/osrg/gobgp/packet/bgp"
    gomrt "github.com/osrg/gobgp/packet/mrt"
	"golang.org/x/net/context"
)

func WriteMRTFile(cmd *cli.Cmd) {
	cmd.Spec = "FILENAME SESSION_ID"
	filename := cmd.StringArg("FILENAME", "", "filename of mrt file")
	sessionID := cmd.StringArg("SESSION_ID", "", "session to write data")

	cmd.Action = func() {
        //open mrt file
        mrtFile, err := os.Open(*filename)
        if err != nil {
            panic(err)
        }

        //open scanner
        scanner := bufio.NewScanner(mrtFile)
        scanner.Split(gomrt.SplitMrt)

        //open stream
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		stream, err := client.Write(ctx)
		if err != nil {
			panic(err)
		}
		defer stream.CloseAndRecv()

        //loop over mrt messsages
        messageCount := 0
        for scanner.Scan() {
            //parse mrt header
            mrtHeader := &gomrt.MRTHeader{}
            data := scanner.Bytes()
            mrtHeader.DecodeFromBytes(data[:gomrt.MRT_COMMON_HEADER_LEN])
            if mrtHeader.Len == 0 {
                panic(errors.New("mrt header length is 0"))
            }

            //parse mrt body
            mrt, err := gomrt.ParseMRTBody(mrtHeader, data[gomrt.MRT_COMMON_HEADER_LEN:])
            if err != nil {
                panic(err)
            }

            switch mrtHeader.Type {
			case gomrt.BGP4MP:
                //parse bgp4mp message
				bgp4mp, ok := mrt.Body.(*gomrt.BGP4MPMessage)
				if !ok {
					fmt.Printf("msg not of type bgp.BGP4MPMessage")
					continue
				}

                //parse bgp4mp header
				bgp4mpHeader := bgp4mp.BGP4MPHeader
				bgpHeader := bgp4mp.BGPMessage.Header
				if bgpHeader.Type != gobgp.BGP_MSG_UPDATE {
					fmt.Printf("msg not an UPDATE. not sending to server")
					continue
				}

				/*bgpdata, err := bgp4mp.BGPMessage.Body.Serialize()
				if err != nil {
					fmt.Printf("Couldn't serialize BGP message:%s", err)
					continue
				}*/

                //populate bgp update message protobuf
                bgpUpdateMessage := new(pb.BGPUpdateMessage)
                bgpUpdateMessage.Timestamp = int64(mrtHeader.Timestamp)
                bgpUpdateMessage.CollectorIpAddress = fmt.Sprintf("%s", bgp4mpHeader.LocalIpAddress)
                //TODO collector mac
                //TODO collector port
                bgpUpdateMessage.PeerIpAddress = fmt.Sprintf("%s", bgp4mpHeader.PeerIpAddress)

                bgpUpdate := bgp4mp.BGPMessage.Body.(*gobgp.BGPUpdate)
                var asPath []uint32
                for _, pathAttribute := range bgpUpdate.PathAttributes {
                    if pathAttribute.GetType() == gobgp.BGP_ATTR_TYPE_AS_PATH {
                        pathAttrASPath := pathAttribute.(*gobgp.PathAttributeAsPath)
                        for _, asPathAttr := range pathAttrASPath.Value {
                            switch asPathParam := asPathAttr.(type) {
                            case *gobgp.AsPathParam:
                                asPath = make([]uint32, asPathParam.Num)
                                for i, asNumber := range asPathParam.AS {
                                    asPath[i] = uint32(asNumber)
                                }
                            case *gobgp.As4PathParam:
                                //because a fallthrough isn't allowed in a type switch
                                asPath = make([]uint32, asPathParam.Num)
                                for i, asNumber := range asPathParam.AS {
                                    asPath[i] = uint32(asNumber)
                                }
                            }
                        }
                    }
                }
                bgpUpdateMessage.AsPath = asPath

                //TODO next hop

                advertisedRoutes := []*pb.IPPrefix{}
                for _, ipAddrPrefix := range bgpUpdate.NLRI {
                    ipPrefix := new(pb.IPPrefix)
                    ipPrefix.PrefixIpAddress = fmt.Sprintf("%v", ipAddrPrefix.Prefix)
                    ipPrefix.PrefixMask = uint32(ipAddrPrefix.Length)
                    advertisedRoutes = append(advertisedRoutes, ipPrefix)
                }
                bgpUpdateMessage.AdvertisedRoutes = advertisedRoutes

                withdrawnRoutes := []*pb.IPPrefix{}
                for _, ipAddrPrefix := range bgpUpdate.WithdrawnRoutes {
                    ipPrefix := new(pb.IPPrefix)
                    ipPrefix.PrefixIpAddress = fmt.Sprintf("%v", ipAddrPrefix.Prefix)
                    ipPrefix.PrefixMask = uint32(ipAddrPrefix.Length)
                    withdrawnRoutes = append(withdrawnRoutes, ipPrefix)
                }
                bgpUpdateMessage.WithdrawnRoutes = withdrawnRoutes

                writeRequest := new(pb.WriteRequest)
				writeRequest.Type = pb.WriteRequest_BGP_UPDATE
				writeRequest.BgpUpdateMessage = bgpUpdateMessage
				writeRequest.SessionId = *sessionID

				if err := stream.Send(writeRequest); err != nil {
                    panic(err)
				}

                messageCount++
                if messageCount % 5000 == 0 {
                    fmt.Printf("processed %d messages\n", messageCount)
                }
			default:
				fmt.Printf("unsupported mrt message type '%v'", mrtHeader.Type)
				continue
			}
		}

		if err := scanner.Err(); err != nil {
            panic(err)
		}

		mrtFile.Close()
	}
}

func WriteASLocationFile(cmd *cli.Cmd) {
	cmd.Spec = "FILENAME SESSION_ID [MEASURE_DATE]"
	filename := cmd.StringArg("FILENAME", "", "filename of as number location file")
	sessionID := cmd.StringArg("SESSION_ID", "", "session to write data")
	measureDate := cmd.StringArg("MEASURE_DATE", time.Now().Format("2006-01-02"), "date of measurement")

	cmd.Action = func() {
		//open file
		file, err := os.Open(*filename)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		//open stream
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		stream, err := client.Write(ctx)
		if err != nil {
			panic(err)
		}
		defer stream.CloseAndRecv()

		//loop through rows
		scanner := bufio.NewScanner(file)
		lineNum := 0
		removeStrings := []string{" ", "'", "{", "}"}
		for scanner.Scan() {
			if scanner.Err() != nil {
				panic(scanner.Err())
			}

			//clean line
			line := strings.Replace(scanner.Text(), "|", ",", -1)
			for _, s := range removeStrings {
				line = strings.Replace(line, s, "", -1)
			}

			fields := strings.Split(line, ",")
			asNumber, err := strconv.ParseInt(fields[0], 10, 64)
			if err != nil {
				panic(err)
			}

			//loop over countries
			for _, country := range fields[1:] {
				if country == "False" {
					break
				}

				location := new(pb.Location)
				location.CountryCode = strings.Trim(strings.TrimSpace(country), "'")

				asNumberLocation := new(pb.ASNumberLocation)
				asNumberLocation.AsNumber = uint32(asNumber)
				asNumberLocation.MeasureDate = *measureDate
				asNumberLocation.Location = location

				writeRequest := new(pb.WriteRequest)
				writeRequest.Type = pb.WriteRequest_AS_NUMBER_LOCATION
				writeRequest.AsNumberLocation = asNumberLocation
				writeRequest.SessionId = *sessionID

				if err = stream.Send(writeRequest); err != nil {
					panic(err)
				}
			}

			lineNum++
			if lineNum % 1000 == 0 {
				fmt.Printf("Processed %d lines\n", lineNum)
			}
		}

		fmt.Printf("Processed %d lines\n", lineNum)
	}
}

func WritePrefixLocationFile(cmd *cli.Cmd) {
	cmd.Spec = "FILENAME SESSION_ID [MEASURE_DATE]"
	filename := cmd.StringArg("FILENAME", "", "filename of as number location file")
	sessionID := cmd.StringArg("SESSION_ID", "", "session to write data")
	measureDate := cmd.StringArg("MEASURE_DATE", time.Now().Format("2006-01-02"), "date of measurement")

	cmd.Action = func() {
		//open file
		file, err := os.Open(*filename)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		//open stream
		client, err := getRPCClient()
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		stream, err := client.Write(ctx)
		if err != nil {
			panic(err)
		}
		defer stream.CloseAndRecv()

		//loop through rows
		scanner := bufio.NewScanner(file)
		lineNum := 0
		replaceStrings := []string{"/","|"}
		removeStrings := []string{" ", "'", "{", "}"}
		for scanner.Scan() {
			if scanner.Err() != nil {
				panic(scanner.Err())
			}

			//clean line
			line := scanner.Text()
			for _, s := range replaceStrings {
				line = strings.Replace(line, s, ",", -1)
			}
			for _, s := range removeStrings {
				line = strings.Replace(line, s, "", -1)
			}

			fields := strings.Split(line, ",")
			prefixMask, err := strconv.ParseUint(fields[1], 10, 32)
			if err != nil {
				panic(err)
			}

			//loop over countries
			for _, country := range fields[2:] {
				if country == "False" {
					break
				}

				location := new(pb.Location)
				location.CountryCode = strings.Trim(strings.TrimSpace(country), "'")

				prefixLocation := new(pb.PrefixLocation)
                ipPrefix := new(pb.IPPrefix)
                ipPrefix.PrefixIpAddress = fields[0]
                ipPrefix.PrefixMask = uint32(prefixMask)
                prefixLocation.Prefix = ipPrefix
				prefixLocation.MeasureDate = *measureDate
				prefixLocation.Location = location

				writeRequest := new(pb.WriteRequest)
				writeRequest.Type = pb.WriteRequest_PREFIX_LOCATION
				writeRequest.PrefixLocation = prefixLocation
				writeRequest.SessionId = *sessionID

				if err = stream.Send(writeRequest); err != nil {
					panic(err)
				}
			}

			lineNum++
			if lineNum % 1000 == 0 {
				fmt.Printf("Processed %d lines\n", lineNum)
			}
		}

		fmt.Printf("Processed %d lines\n", lineNum)
	}
}
