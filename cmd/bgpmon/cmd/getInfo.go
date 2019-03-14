package cmd

import (
	"fmt"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/spf13/cobra"
)

// listAvailableCmd represents the listAvailable command
var getInfoCmd = &cobra.Command{
	Use:   "getInfo <ID>",
	Short: "Returns the info of session with id <ID>",
	Long:  `Prints the session type (Type, Name, Description), ID, and worker count of a session`,
	Args:  cobra.ExactArgs(1),
	Run:   getInfoFunc,
}

func getInfoFunc(cmd *cobra.Command, args []string) {
	bc, clierr := newBgpmonCli(bgpmondHost, bgpmondPort)
	if clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
	}

	defer bc.close()
	msg := &pb.SessionInfoRequest{SessionId: args[0]}
	ctx, cancel := getCtxWithCancel()
	defer cancel()

	reply, err := bc.cli.GetSessionInfo(ctx, msg)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
	}

	fmt.Printf("ID: %s\n", reply.SessionId)
	fmt.Printf("Workers: %d\n", reply.Workers)
	fmt.Printf("Name: %s\n", reply.Type.Name)
	fmt.Printf("Type: %s\n", reply.Type.Type)
	fmt.Printf("Description: %s\n", reply.Type.Desc)

}

func init() {
	rootCmd.AddCommand(getInfoCmd)
}
