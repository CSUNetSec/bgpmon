package cmd

import (
	"fmt"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/spf13/cobra"
)

// closeCmd represents the close command
var closeCmd = &cobra.Command{
	Use:   "close SESS_ID",
	Short: "Closes a session.",
	Long:  `Closes a currently open session named SESS_ID.`,
	Args:  cobra.ExactArgs(1),
	Run:   closeSess,
}

// the *cobra.Command is necessary for cobra but isn't used.
func closeSess(_ *cobra.Command, args []string) {
	sessID := args[0]
	bc, clierr := newBgpmonCli(bgpmondHost, bgpmondPort)
	if clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
		return
	}
	defer bc.close()
	emsg := &pb.CloseSessionRequest{
		SessionId: sessID,
	}
	ctx, cancel := getCtxWithCancel()
	defer cancel()
	reply, err := bc.cli.CloseSession(ctx, emsg)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	fmt.Println("closed session with ID:", sessID, " server replied: ", reply)
}

func init() {
	rootCmd.AddCommand(closeCmd)
}
