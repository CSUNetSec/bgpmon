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
	Use:              "read",
	Short:            "Reads filtered captures, prefixes and as-paths from a bgpmond server.",
	Long:             "Reads filtered captures, prefixes and as-paths from a bgpmond server.",
	TraverseChildren: true,
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
	return os.Create(output)
}

var readCapCmd = &cobra.Command{
	Use:   "count SESS_ID FILTER",
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

	msg := 0

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
		msg++

	}

	fmt.Printf("Total messages: %d\n", msg)
	return
}

var readPrefixCmd = &cobra.Command{
	Use:   "prefix SESS_ID FILTER",
	Short: "Reads advertized prefixes from a bgpmond server.",
	Long: `Constructs a filter from the provided filter string, opens a read stream on a bgpmond server,
	and reads all prefixes passing the filter.`,
	Run:  readPrefixes,
	Args: cobra.MinimumNArgs(1),
}

func readPrefixes(_ *cobra.Command, args []string) {
	sessID := args[0]

	start, end, err := getTimeSpan()
	if err != nil {
		fmt.Printf("Error parsing time span: %s\n", err)
		return
	}

	if len(args) > 1 {
		// Parse filters here
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
		Type:           pb.GetRequest_PREFIX,
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

	fd, err := getOutputFile()
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}
	defer fd.Close()

	msg := 0

	for {
		resp, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Error reading from stream: %s\n", err)
			}
			break
		}

		if resp.Error != "" {
			fmt.Printf("Stream returned error: %s\n", resp.Error)
			break
		}
		for _, v := range resp.Chunk {
			fmt.Fprintf(fd, "%s\n", string(v))
		}
		msg++
	}

	fmt.Printf("Total messages: %d\n", msg)
	return
}

func init() {
	readCmd.AddCommand(readCapCmd)
	readCmd.AddCommand(readPrefixCmd)

	readCmd.PersistentFlags().StringVarP(&output, "output", "o", "", "output file to store the read data")
	readCmd.PersistentFlags().StringVarP(&collector, "collector", "c", "", "collector to read from (required)")
	readCmd.MarkFlagRequired("collector")
	readCmd.PersistentFlags().StringVarP(&startStr, "start", "s", "", "beginning time of the read (required)")
	readCmd.MarkFlagRequired("start")
	readCmd.PersistentFlags().StringVarP(&endStr, "end", "e", "", "end time of the read (required)")
	readCmd.MarkFlagRequired("end")

	rootCmd.AddCommand(readCmd)
}
