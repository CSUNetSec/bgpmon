package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/CSUNetSec/bgpmon/protobuf"

	cli "github.com/jawher/mow.cli"
	"golang.org/x/net/context"
)

func WriteMRTFile(cmd *cli.Cmd) {
	cmd.Spec = "FILENAME SESSION_ID"
	filename := cmd.StringArg("FILENAME", "", "filename of mrt file")
	sessionID := cmd.StringArg("SESSION_ID", "", "session to write data")

	cmd.Action = func() {
		fmt.Printf("TODO write bgp messages from mrt file %s to session %s\n", *filename, *sessionID)
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
