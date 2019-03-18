package cmd

import (
	"fmt"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/spf13/cobra"
)

var listAvailableCmd = &cobra.Command{
	Use:   "listAvailable",
	Short: "Lists configured types of sessions and their names.",
	Long: `Lists the names and information of the sessions configured in a bgpmond server.
These names can be used with the open command to open these types of sessions.`,
	Args: cobra.NoArgs,
	Run:  listAvailable,
}

// the first argument is required by cobra but not used.
func listAvailable(_ *cobra.Command, args []string) {
	bc, clierr := newBgpmonCli(bgpmondHost, bgpmondPort)
	if clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
		return
	}
	defer bc.close()
	emsg := &pb.Empty{}
	ctx, cancel := getCtxWithCancel()
	defer cancel()
	reply, err := bc.cli.ListAvailableSessions(ctx, emsg)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	fmt.Printf("%d Available Sessions:\n", len(reply.AvailableSessions))
	for i, as := range reply.AvailableSessions {
		fmt.Printf("[%d]\n\tName:%s\n\tType:%s\n\tDescription:%v\n", i, as.Name, as.Type, as.Desc)
	}
}

func init() {
	rootCmd.AddCommand(listAvailableCmd)
}
