package cmd

import (
	"fmt"
	"io"
	"time"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/araddon/dateparse"
	"github.com/spf13/cobra"
)

var (
	oas              uint32    //origin as filter
	ststamp, etstamp time.Time //start and end timestamps
	ststr, etstr     string    //timestamps before they get parsed
	collectorstr     string    //the collector to get info from
)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get",
	Short: "fetches filtered captures, prefixes and as-paths from bgpmond",
	Long: `get requires filters to bring back a number of either captures, prefixes, or aspaths
from bgpmond. Filters startTime and endTime should be specified together. Time layout should be
in RFC3339 form like 2006-01-02T15:04:05Z.
`,
	RunE: get,
}

func get(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("argument for session needed")
	}
	sessID := args[0]
	//populate Filters
	/*
		filts := []pb.Filter{}
		if cmd.Flags().Changed("originAs") {
			filts = append(filts, pb.Filter{Type: pb.Filter_ORIGIN_AS, OriginAs: oas})
		}
	*/

	ac := cmd.Flags().Changed("startTime")
	bc := cmd.Flags().Changed("endTime")
	if !ac && !bc {
		return fmt.Errorf("start and end time needs to be provided")
	}
	//make sure they were both specified
	if !(ac && bc) { //they must be specified together. else error
		return fmt.Errorf("start, end time must be specified together")
	}
	t1, err1 := dateparse.ParseAny(ststr)
	t2, err2 := dateparse.ParseAny(etstr)
	if err1 != nil || err2 != nil {
		return fmt.Errorf("failed to parse the provided time spec")
	}
	moncli, clierr := newBgpmonCli(bgpmondHost, bgpmondPort)
	if clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
		return clierr
	}
	defer moncli.Close()
	ctx, cancel := getBackgroundCtxWithCancel()
	// First get the session info
	pbr := &pb.GetRequest{Type: pb.GetRequest_CAPTURE, SessionId: sessID,
		CollectorName: collectorstr, StartTimestamp: uint64(t1.Unix()), EndTimestamp: uint64(t2.Unix())}
	stream, err := moncli.cli.Get(ctx, pbr)
	if err != nil {
		fmt.Printf("Error getting session info: %s\n", err)
		cancel()
		return err
	}
	for {
		thing, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			break
		}
		fmt.Printf("received a thing:%v", thing)

	}
	return nil
}

func init() {
	rootCmd.AddCommand(getCmd)
	getCmd.PersistentFlags().Uint32VarP(&oas, "originAs", "o", 0, "origin as filter")
	getCmd.PersistentFlags().StringVarP(&ststr, "startTime", "s", "", "startTimestamp")
	getCmd.PersistentFlags().StringVarP(&etstr, "endTime", "e", "", "endTimestamp")
	getCmd.PersistentFlags().StringVarP(&collectorstr, "collector", "n", "routeviews2", "collector node to query data from")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// getCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// getCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
