package cmd

import (
	"fmt"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"

	"github.com/spf13/cobra"
)

// listOpenCmd represents the listOpen command
var listOpenCmd = &cobra.Command{
	Use:   "listOpen",
	Short: "list the open session IDs that the bgpmond currently handles",
	Long: `Lists the IDs that the bgpmond uses to refer to individual open
sessions, this can be used to perform bgpmon queries on them or close them.`,
	Args: cobra.NoArgs,
	Run:  listOpen,
}

func listOpen(cmd *cobra.Command, args []string) {
	if bc, clierr := newBgpmonCli(bgpmondHost, bgpmondPort); clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
	} else {
		defer bc.Close()
		emsg := &pb.Empty{}
		ctx, cancel := getCtxWithCancel()
		defer cancel()
		if reply, err := bc.cli.ListOpenSessions(ctx, emsg); err != nil {
			fmt.Printf("Error: %s\n", err)
		} else {
			fmt.Printf("%d Open Sessions:\n", len(reply.SessionId))
			for i, as := range reply.SessionId {
				fmt.Printf("[%d]\tID:%s\n", i, as)
			}
		}

	}

}
func init() {
	rootCmd.AddCommand(listOpenCmd)
}
