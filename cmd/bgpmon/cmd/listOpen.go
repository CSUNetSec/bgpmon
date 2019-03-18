package cmd

import (
	"fmt"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/spf13/cobra"
)

var listOpenCmd = &cobra.Command{
	Use:   "listOpen",
	Short: "Lists the open session IDs on the bgpmond server.",
	Long: `Lists the open session IDs on the bgpmond server. 
These can be used as arguments bgpmon queries like get or write, or close them.`,
	Args: cobra.NoArgs,
	Run:  listOpen,
}

// the first argument is required by cobra but not used.
func listOpen(_ *cobra.Command, args []string) {
	bc, clierr := newBgpmonCli(bgpmondHost, bgpmondPort)
	if clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
		return
	}
	defer bc.close()
	emsg := &pb.Empty{}
	ctx, cancel := getCtxWithCancel()
	defer cancel()
	reply, err := bc.cli.ListOpenSessions(ctx, emsg)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	fmt.Printf("%d Open Sessions:\n", len(reply.SessionId))
	for i, openSess := range reply.SessionId {
		fmt.Printf("[%d]\tID:%s\n", i, openSess)
	}
}

func init() {
	rootCmd.AddCommand(listOpenCmd)
}
