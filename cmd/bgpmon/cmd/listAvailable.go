package cmd

import (
	"fmt"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/spf13/cobra"
)

// listAvailableCmd represents the listAvailable command
var listAvailableCmd = &cobra.Command{
	Use:   "listAvailable",
	Short: "list configured types of sessions and their names",
	Long: `Lists the names and information of the sessions configured in a bgpmond
these names can be used with the open command to open these types of sessions`,
	Args: cobra.NoArgs,
	Run:  listAvail,
}

func listAvail(cmd *cobra.Command, args []string) {
	fmt.Printf("listAvailable called with args:%s\n", args)
	if bc, clierr := newBgpmonCli(bgpmondHost, bgpmondPort); clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
	} else {
		defer bc.close()
		emsg := &pb.Empty{}
		ctx, cancel := getCtxWithCancel()
		defer cancel()
		if reply, err := bc.cli.ListAvailableSessions(ctx, emsg); err != nil {
			fmt.Printf("Error: %s\n", err)
		} else {
			fmt.Printf("%d Available Sessions:\n", len(reply.AvailableSessions))
			for i, as := range reply.AvailableSessions {
				fmt.Printf("[%d]\n\tName:%s\n\tType:%s\n\tDescription:%v\n", i, as.Name, as.Type, as.Desc)
			}
		}

	}
}

func init() {
	rootCmd.AddCommand(listAvailableCmd)
}
