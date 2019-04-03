package cmd

import (
	"fmt"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/spf13/cobra"
)

var closeCmd = &cobra.Command{
	Use:   "close",
	Short: "Close a session or module on a bgpmond server.",
	Long:  "Close a session or module on a bgpmond server.",
}

var closeSessionCmd = &cobra.Command{
	Use:   "session ID",
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
	_, err := bc.cli.CloseSession(ctx, emsg)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	fmt.Printf("Closed session: %s\n", sessID)
}

var closeModuleCmd = &cobra.Command{
	Use:   "module ID",
	Short: "Closes a module.",
	Long:  "Closes a running module with ID ID",
	Args:  cobra.ExactArgs(1),
	Run:   closeModule,
}

func closeModule(_ *cobra.Command, args []string) {
	modID := args[0]
	bc, clierr := newBgpmonCli(bgpmondHost, bgpmondPort)
	if clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
		return
	}
	defer bc.close()
	emsg := &pb.CloseModuleRequest{
		Id: modID,
	}
	ctx, cancel := getCtxWithCancel()
	defer cancel()
	_, err := bc.cli.CloseModule(ctx, emsg)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	fmt.Printf("Closed module: %s\n", modID)
}

func init() {
	closeCmd.AddCommand(closeSessionCmd)
	closeCmd.AddCommand(closeModuleCmd)
	rootCmd.AddCommand(closeCmd)
}
