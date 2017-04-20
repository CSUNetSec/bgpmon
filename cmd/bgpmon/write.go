package main

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"fmt"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon"
	pp "github.com/CSUNetSec/protoparse"
	ppmrt "github.com/CSUNetSec/protoparse/protocol/mrt"
	pputil "github.com/CSUNetSec/protoparse/util"
	radix "github.com/armon/go-radix"
	cli "github.com/jawher/mow.cli"
	"golang.org/x/net/context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func getScanner(file *os.File) (scanner *bufio.Scanner) {
	fname := file.Name()
	fext := filepath.Ext(fname)
	if fext == ".bz2" {
		bzreader := bzip2.NewReader(file)
		scanner = bufio.NewScanner(bzreader)
	} else {
		scanner = bufio.NewScanner(file)
	}
	scanner.Split(ppmrt.SplitMrt)
	scanbuffer := make([]byte, 2<<20) //an internal buffer for the large tokens (1M)
	scanner.Buffer(scanbuffer, cap(scanbuffer))
	return
}

func IpToRadixkey(b []byte, mask uint8) string {
	var buffer bytes.Buffer
	for i := 0; i < len(b) && i < int(mask); i++ {
		buffer.WriteString(fmt.Sprintf("%08b", b[i]))
	}
	return buffer.String()[:mask]
}

func maskstr2uint8(m string) (uint8, error) {
	mask, err := strconv.ParseUint(m, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint8(mask), nil
}

func WriteMRTFile(cmd *cli.Cmd) {
	cmd.Spec = "FILENAME SESSION_ID PREFIX_FILE"
	filename := cmd.StringArg("FILENAME", "", "filename of mrt file")
	sessionID := cmd.StringArg("SESSION_ID", "", "session to write data")
	prefixfile := cmd.StringArg("PREFIX_FILE", "", "Filename that contains prefixes of interest to be written")

	var rt *radix.Tree
	cmd.Action = func() {
		//open mrt file
		if *prefixfile != "" {
			pfile, err := os.Open(*prefixfile)
			if err != nil {
				panic(err)
			}
			defer pfile.Close()
			ls := bufio.NewScanner(pfile)
			rt = radix.New()
			for ls.Scan() {
				parts := strings.Split(ls.Text(), ",")
				if len(parts) != 4 {
					fmt.Printf("malformed line %s\n", ls.Text())
					continue
				}
				mask, err := maskstr2uint8(parts[2])
				if err != nil {
					fmt.Printf("error parsing mask:%s err:%s", parts[2], err)
					continue
				}
				rt.Insert(IpToRadixkey(net.ParseIP(parts[1]).To4(), mask), true)
			}
			if err := ls.Err(); err != nil {
				fmt.Printf("prefix file scanner error:%s\n", err)
			}
			fmt.Printf("Set up a prefix tree with %d entries\n", rt.Len())
		}

		mrtFile, err := os.Open(*filename)
		if err != nil {
			panic(err)
		}
		defer mrtFile.Close()

		//open scanner
		scanner := getScanner(mrtFile)

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
		startTime := time.Now()
		headerLengthZeroCount, messageCount, unableToParseBodyCount, noAsPathCount, notBGPUpdateCount, monitoredCount, notMonitoredCount := 0, 0, 0, 0, 0, 0, 0
		for scanner.Scan() {
			messageCount++
			data := scanner.Bytes()
			mrth := ppmrt.NewMrtHdrBuf(data)
			bgp4h, errmrt := mrth.Parse()
			if errmrt != nil {
				notBGPUpdateCount++
				fmt.Printf("Failed parsing MRT header %d :%s\n", messageCount, errmrt)
				continue
			}
			bgph, errbgph := bgp4h.Parse()
			if errbgph != nil {
				notBGPUpdateCount++
				fmt.Printf("Failed parsing BGP4MP header %d :%s\n", messageCount, errbgph)
				continue
			}
			bgpup, errbgpup := bgph.Parse()
			if errbgpup != nil {
				headerLengthZeroCount++
				fmt.Printf("Failed parsing BGP Header  %d :%s\n", messageCount, errbgpup)
				continue
			}
			_, errup := bgpup.Parse()
			if errup != nil {
				unableToParseBodyCount++
				fmt.Printf("Failed parsing BGP Update  %d :%s\n", messageCount, errup)
				continue
			}
			capture := new(pb.BGPCapture)
			bgphpb := bgp4h.(pp.BGP4MPHeaderer).GetHeader()
			mrtpb := mrth.GetHeader()
			capture.Timestamp = mrtpb.Timestamp
			capture.PeerAs = bgphpb.PeerAs
			capture.LocalAs = bgphpb.LocalAs
			capture.InterfaceIndex = bgphpb.InterfaceIndex
			capture.AddressFamily = bgphpb.AddressFamily
			capture.PeerIp = bgphpb.PeerIp
			capture.LocalIp = bgphpb.LocalIp
			capture.Update = bgpup.(pp.BGPUpdater).GetUpdate()
			if capture.Update == nil {
				fmt.Printf("\n Update is nil! \n")
			}
			adr := capture.Update.GetAdvertizedRoutes()
			//look for advertized routes with no as path
			if att := capture.Update.GetAttrs(); att != nil {
				if len(att.GetAsPath()) == 0 && adr != nil && len(adr.GetPrefixes()) != 0 {
					fmt.Printf("no AS_PATH info on message:%d while advertised routes are present\n", messageCount)
					noAsPathCount++
					continue
				}
			}
			//look only for prefixes of interest
			monitoring_update := false
			if rt == nil { // noone started the radix tree. monitoring all prefixes
				monitoring_update = true
			}

			if adr != nil && len(adr.GetPrefixes()) != 0 && rt != nil {
				prefixes := adr.GetPrefixes()
				for i := range prefixes {
					if _, _, found := rt.LongestPrefix(IpToRadixkey(prefixes[i].GetPrefix().GetIpv4(), uint8(prefixes[i].GetMask()))); found {
						monitoring_update = true
					}
				}
			}
			if monitoring_update {
				monitoredCount++
				writeRequest := new(pb.WriteRequest)
				writeRequest.Type = pb.WriteRequest_BGP_CAPTURE
				writeRequest.BgpCapture = capture
				writeRequest.SessionId = *sessionID
				if err := stream.Send(writeRequest); err != nil {
					fmt.Println("FOUND ERROR")
					panic(err)
				}
			}
			if !monitoring_update && rt != nil {
				notMonitoredCount++
			}
			/*if messageCount%1000 == 0 {
				fmt.Printf("message:%d time elapsed:%v\n", messageCount, time.Since(startTime))
			}*/

		}

		if err := scanner.Err(); err != nil {
			fmt.Printf("Failed to parse message:%s", err)
		}

		fmt.Printf("time_elapsed:%v total_messages:%d messages/second:%f notUpdate:%d FailedParse:%d noAsPathCount:%d monitored:%d notMonitored:%d\n",
			time.Since(startTime), messageCount, float64(messageCount)/time.Since(startTime).Seconds(), notBGPUpdateCount, unableToParseBodyCount, noAsPathCount, monitoredCount, notMonitoredCount)
		/*fmt.Printf("processed %d total messages in %v\n"+
		"\theaderLengthZeroCount:%d\n"+
		"\tunableToParseBodyCount:%d\n"+
		"\tnotBGPUpdateCount:%d\n"+
		"\taUnixsPathLengthZeroCount:%d\n",
		messageCount,
		time.Since(startTime),
		headerLengthZeroCount,
		unableToParseBodyCount,
		notBGPUpdateCount,
		asPathLengthZeroCount)*/
	}
}

func WriteBGPHistory(cmd *cli.Cmd) {
	cmd.Spec = "FILENAME SESSION_ID"
	filename := cmd.StringArg("FILENAME", "", "filename of mrt file")
	sessionId := cmd.StringArg("SESSION_ID", "", "session to write data")

	//timestamp := uint32(0)

	cmd.Action = func() {
		//open mrt file
		mrtFile, err := os.Open(*filename)
		if err != nil {
			panic(err)
		}
		defer mrtFile.Close()

		//open scanner
		scanner := getScanner(mrtFile)

		//create map for storing information (collector_ip -> as_number -> ip_address -> mask -> count
		//collectorMap := make(map[string]map[uint32]map[string]map[int]uint32)

		//loop over mrt messsages
		messageCount := 0
		startTime := time.Now()
		var (
			stats  *pb.BGPStats
			exists bool
		)
		colstats := make(map[string]*pb.BGPStats)
		for scanner.Scan() {
			//parse mrt header
			data := scanner.Bytes()
			mrth := ppmrt.NewMrtHdrBuf(data)
			bgp4h, errmrt := mrth.Parse()
			if errmrt != nil {
				fmt.Printf("Failed parsing MRT header %d :%s\n", messageCount, errmrt)
				continue
			}
			bgph, errbgph := bgp4h.Parse()
			if errbgph != nil {
				fmt.Printf("Failed parsing BGP4MP header %d :%s\n", messageCount, errbgph)
				continue
			}
			bgpup, errbgpup := bgph.Parse()
			if errbgpup != nil {
				fmt.Printf("Failed parsing BGP Header  %d :%s\n", messageCount, errbgpup)
				continue
			}
			_, errup := bgpup.Parse()
			if errup != nil {
				fmt.Printf("Failed parsing BGP Update  %d :%s\n", messageCount, errup)
				continue
			}
			bgphpb := bgp4h.(pp.BGP4MPHeaderer).GetHeader()
			mrtpb := mrth.GetHeader()
			upd := bgpup.(pp.BGPUpdater).GetUpdate()
			if upd == nil {
				fmt.Printf("\n Update is nil! \n")
				continue
			}
			adr := upd.GetAdvertizedRoutes()

			colip := net.IP(pputil.GetIP(bgphpb.LocalIp))
			ipstr := colip.String()
			if stats, exists = colstats[ipstr]; !exists {
				stats = new(pb.BGPStats)
				stats.TimeStartSecEpoch = uint64(mrtpb.Timestamp)
				stats.DurationSec = (15 * 60)
				stats.MsgCounts = new(pb.Count)
				stats.MsgCounts.Count = make(map[string]uint32)
				stats.DestAsToASPCounts = make(map[string]*pb.Count)
				stats.DestAsToPrefixCounts = make(map[string]*pb.Count)
				colstats[ipstr] = stats
			}
			messageCount++
			stats.MsgCounts.Count["UpdateCount"] = uint32(messageCount)
			var asPath []uint32
			if att := upd.GetAttrs(); att != nil {
				if asp := att.GetAsPath(); len(asp) != 0 {
					for _, seg := range asp {
						if seg.AsSeq != nil {
							for _, as := range seg.AsSeq {
								asPath = append(asPath, as)
							}
						}
						if seg.AsSet != nil {
							//XXX: actually create more aspaths with all set elements
							asPath = append(asPath, seg.AsSet[0])
						}
					}

				}
			}
			if len(asPath) == 0 {
				continue //as path length = 0
			}
			asNumber := asPath[len(asPath)-1]
			asstr := fmt.Sprintf("%d", asNumber)
			destasp, exists := stats.DestAsToASPCounts[asstr]
			if !exists {
				destasp = new(pb.Count)
				destasp.Count = make(map[string]uint32)
			}
			for i := 0; i < len(asPath)-1; i++ {
				key := fmt.Sprintf("%d,%d", asPath[i], asPath[i+1])
				if num, exval := destasp.Count[key]; exval {
					destasp.Count[key] = num + 1
				} else {
					destasp.Count[key] = 1
				}
			}
			stats.DestAsToASPCounts[asstr] = destasp
			if adr != nil && len(adr.GetPrefixes()) != 0 {
				destpr, exists := stats.DestAsToPrefixCounts[asstr]
				if !exists {
					destpr = new(pb.Count)
					destpr.Count = make(map[string]uint32)
				}
				prefixes := adr.GetPrefixes()
				for i := range prefixes {
					ipAddress := fmt.Sprintf("%s", net.IP(pputil.GetIP(prefixes[i].GetPrefix())))
					mask := int(prefixes[i].GetMask())
					key := fmt.Sprintf("%s/%d", ipAddress, mask)
					if num, exval := destpr.Count[key]; exval {
						destpr.Count[key] = num + 1
					} else {
						destpr.Count[key] = 1
					}
				}
				stats.DestAsToPrefixCounts[asstr] = destpr
			}
		}

		if err := scanner.Err(); err != nil {
			panic(err)
		}

		mrtFile.Close()
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

		//send write request
		for _, v := range colstats {
			writeRequest := new(pb.WriteRequest)
			writeRequest.Type = pb.WriteRequest_BGP_STATS
			writeRequest.BgpStats = v
			writeRequest.SessionId = *sessionId

			if err := stream.Send(writeRequest); err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}
		}
		fmt.Printf("processed %d total messages in %v. sent %d messages to the server\n", messageCount, time.Since(startTime), len(colstats))
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
			if lineNum%1000 == 0 {
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
		replaceStrings := []string{"/", "|"}
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
			if lineNum%1000 == 0 {
				fmt.Printf("Processed %d lines\n", lineNum)
			}
		}

		fmt.Printf("Processed %d lines\n", lineNum)
	}
}
