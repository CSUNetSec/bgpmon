package cmd

import (
	"fmt"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/spf13/cobra"
)

var listAvailableCmd = &cobra.Command{
	Use:   "listAvailable",
	Short: "Lists available sessions or modules on a bgpmond server.",
	Long:  "Lists available sessions or modules on a bgpmond server.",
}

var listAvailableSessionsCmd = &cobra.Command{
	Use:   "sessions",
	Short: "Lists configured types of sessions and their names.",
	Long: `Lists the names and information of the sessions configured in a bgpmond server.
These names can be used with the open command to open these types of sessions.`,
	Args: cobra.NoArgs,
	Run:  listAvailableSessions,
}

// The arguments are required by cobra but not used.
func listAvailableSessions(_ *cobra.Command, _ []string) {
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
	fmt.Printf("Available Sessions: %d\n", len(reply.AvailableSessions))
	for i, as := range reply.AvailableSessions {
		fmt.Printf("[%d]\n", i)
		fmt.Printf("Name:        %s\n", as.Name)
		fmt.Printf("Type:        %s\n", as.Type)
		fmt.Printf("Description: %s\n", as.Desc)
	}
}

var listAvailableModulesCmd = &cobra.Command{
	Use:   "modules",
	Short: "Lists registered modules on a bgpmond server.",
	Long:  "Lists type, description, and options for each module registered to a bgpmond server",
	Args:  cobra.NoArgs,
	Run:   listAvailableModules,
}

func listAvailableModules(_ *cobra.Command, _ []string) {
	bc, clierr := newBgpmonCli(bgpmondHost, bgpmondPort)
	if clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
		return
	}
	defer bc.close()
	emsg := &pb.Empty{}
	ctx, cancel := getCtxWithCancel()
	defer cancel()
	reply, err := bc.cli.ListAvailableModules(ctx, emsg)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	fmt.Printf("Available Modules: %d\n", len(reply.AvailableModules))
	for i, mod := range reply.AvailableModules {
		fmt.Printf("[%d]\n", i)
		fmt.Printf("Type:        %s\n", mod.Type)
		fmt.Printf("Description: %s\n", mod.Desc)
		fmt.Printf("Options:     \n%s\n", mod.Opts)
	}
}

func init() {
	listAvailableCmd.AddCommand(listAvailableSessionsCmd)
	listAvailableCmd.AddCommand(listAvailableModulesCmd)
	rootCmd.AddCommand(listAvailableCmd)
}
