package cmd

import (
	"fmt"
	"io"
	"os"
	"time"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/araddon/dateparse"
	"github.com/spf13/cobra"
)

// Variables to store flags
var (
	output    string
	collector string
	startStr  string
	endStr    string
)

// readCmd is a wrapper for read capture, read prefixes, etc. It defines
// persistent flags, which are shared by all subcommands of read.
var readCmd = &cobra.Command{
	Use:   "read",
	Short: "Reads filtered captures, prefixes and as-paths from a bgpmond server.",
	Long:  "Reads filtered captures, prefixes and as-paths from a bgpmond server.",
}

func getTimeSpan() (start time.Time, end time.Time, err error) {
	start, err = dateparse.ParseAny(startStr)
	if err != nil {
		return
	}
	end, err = dateparse.ParseAny(endStr)
	return
}

func getOutputFile() (*os.File, error) {
	if output == "" {
		return os.Stdout, nil
	}
	return os.Open(output)
}

var readCapCmd = &cobra.Command{
	Use:   "capture SESS_ID FILTER",
	Short: "Reads bgp captures from a bgpmond server.",
	Long: `Constructs a filter from the provided filter string, opens a read stream on a bgpmond server,
	and reads all captures passing the filter.`,
	Run:  readCapture,
	Args: cobra.MinimumNArgs(1),
}

// The cobra command is required, but not used.
func readCapture(_ *cobra.Command, args []string) {
	sessID := args[0]

	start, end, err := getTimeSpan()
	if err != nil {
		fmt.Printf("Error parsing time span: %s\n", err)
		return
	}

	if len(args) > 1 {
		// Parse filters here
	}

	outputFd, err := getOutputFile()
	if err != nil {
		fmt.Printf("Error opening output file: %s\n", err)
		return
	}

	moncli, clierr := newBgpmonCli(bgpmondHost, bgpmondPort)
	if clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
		return
	}
	defer moncli.close()

	ctx, cancel := getBackgroundCtxWithCancel()
	defer cancel()

	getReq := &pb.GetRequest{
		Type:           pb.GetRequest_CAPTURE,
		SessionId:      sessID,
		CollectorName:  collector,
		StartTimestamp: uint64(start.Unix()),
		EndTimestamp:   uint64(end.Unix()),
	}

	stream, err := moncli.cli.Get(ctx, getReq)
	if err != nil {
		fmt.Printf("Error opening RPC stream: %s\n", err)
		return
	}

	for {
		cap, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Error reading from stream: %s\n", err)
			}
			break
		}

		if cap.Error != "" {
			fmt.Printf("Stream returned error: %s\n", cap.Error)
			break
		}

		for _, v := range cap.Chunk {
			_, err = outputFd.Write(v)
			if err != nil {
				fmt.Printf("Error writing to output file: %s\n", err)
				return
			}
		}

	}
	return
}

func init() {
	readCmd.AddCommand(readCapCmd)

	readCmd.PersistentFlags().StringVarP(&output, "output", "o", "", "output file to store the read data")
	readCmd.PersistentFlags().StringVarP(&collector, "collector", "c", "", "collector to read from (required)")
	readCmd.MarkFlagRequired("collector")
	readCmd.PersistentFlags().StringVarP(&startStr, "start", "s", "", "beginning time of the read (required)")
	readCmd.MarkFlagRequired("start")
	readCmd.PersistentFlags().StringVarP(&endStr, "end", "e", "", "end time of the read (required)")
	readCmd.MarkFlagRequired("end")

	rootCmd.AddCommand(readCmd)
}
