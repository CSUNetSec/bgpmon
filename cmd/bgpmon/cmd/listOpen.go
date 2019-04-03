package cmd

import (
	"fmt"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/spf13/cobra"
)

// This is a wrapper for listOpenSessions and listOpenModules.
// The Short and Long are the same becuase the relevant details
// are in the subcommands.
var listOpenCmd = &cobra.Command{
	Use:   "listOpen",
	Short: "Lists open modules or sessions on the server.",
	Long:  "Lists open modules or sessions on the server.",
}

var listOpenSessionsCmd = &cobra.Command{
	Use:   "sessions",
	Short: "Lists the open session IDs on the bgpmond server.",
	Long: `Lists the open session IDs on the bgpmond server. 
These can be used as arguments bgpmon queries like get or write, or close them.`,
	Args: cobra.NoArgs,
	Run:  listOpenSessions,
}

// The first argument is required by cobra but not used.
func listOpenSessions(_ *cobra.Command, args []string) {
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
	fmt.Printf("Open Sessions: %d\n", len(reply.SessionId))
	for i, openSess := range reply.SessionId {
		fmt.Printf("[%d]\nID: %s\n", i, openSess)
	}
}

var listOpenModulesCmd = &cobra.Command{
	Use:   "modules",
	Short: "Lists the open modules on the bgpmond server.",
	Long:  "Lists the ID, type, and status of all open modules on the bgpmond server",
	Args:  cobra.NoArgs,
	Run:   listOpenModules,
}

// The first argument is required by cobra but unused.
func listOpenModules(_ *cobra.Command, args []string) {
	bc, clierr := newBgpmonCli(bgpmondHost, bgpmondPort)
	if clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
		return
	}
	defer bc.close()
	emsg := &pb.Empty{}
	ctx, cancel := getCtxWithCancel()
	defer cancel()
	reply, err := bc.cli.ListOpenModules(ctx, emsg)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	fmt.Printf("Open Modules: %d\n", len(reply.OpenModules))
	for i, openMod := range reply.OpenModules {
		fmt.Printf("[%d]\n", i)
		fmt.Printf("Type:   %s\n", openMod.Type)
		fmt.Printf("ID:     %s\n", openMod.Id)
		fmt.Printf("Status: %s\n", openMod.Status)
	}
}

func init() {
	listOpenCmd.AddCommand(listOpenSessionsCmd)
	listOpenCmd.AddCommand(listOpenModulesCmd)
	rootCmd.AddCommand(listOpenCmd)
}
