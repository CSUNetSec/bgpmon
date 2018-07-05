package cmd

import (
	"fmt"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/spf13/cobra"
)

// closeCmd represents the close command
var closeCmd = &cobra.Command{
	Use:   "close",
	Short: "close a session ID",
	Long:  `closes a session associated with an ID that is provided.`,
	Run:   closeSess,
}

func closeSess(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println("Error: requiring session ID to close")
		return
	}
	if bc, clierr := NewBgpmonCli(bgpmondHost, bgpmondPort); clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
	} else {
		defer bc.Close()
		emsg := &pb.CloseSessionRequest{
			SessionId: args[0],
		}
		ctx, cancel := getCtxWithCancel()
		defer cancel()
		if reply, err := bc.cli.CloseSession(ctx, emsg); err != nil {
			fmt.Printf("Error: %s\n", err)
		} else {
			fmt.Println("closed session with ID:", args[0], " server replied: ", reply)
		}
	}

}

func init() {
	rootCmd.AddCommand(closeCmd)
}
