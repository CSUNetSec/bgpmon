package cmd

import (
	"fmt"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/spf13/cobra"
)

var getInfoCmd = &cobra.Command{
	Use:   "getInfo SESS_ID",
	Short: "Returns the info of an open session named SESS_ID.",
	Long:  `Prints the type, name, description, ID, and worker count of an open session named SESS_ID.`,
	Args:  cobra.ExactArgs(1),
	Run:   getInfoFunc,
}

// the first argument is required by cobra but not used.
func getInfoFunc(_ *cobra.Command, args []string) {
	bc, clierr := newBgpmonCli(bgpmondHost, bgpmondPort)
	if clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
		return
	}

	defer bc.close()
	msg := &pb.SessionInfoRequest{SessionId: args[0]}
	ctx, cancel := getCtxWithCancel()
	defer cancel()

	reply, err := bc.cli.GetSessionInfo(ctx, msg)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	fmt.Printf("ID:          %s\n", reply.SessionId)
	fmt.Printf("Workers:     %d\n", reply.Workers)
	fmt.Printf("Name:        %s\n", reply.Type.Name)
	fmt.Printf("Type:        %s\n", reply.Type.Type)
	fmt.Printf("Description: %s\n", reply.Type.Desc)
}

func init() {
	rootCmd.AddCommand(getInfoCmd)
}
