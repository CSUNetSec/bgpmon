package cmd

import (
	"fmt"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

var (
	sID string // sID is the ID  for the open session request.
	nw  uint32 // nw is the number of maximum database workers.
)

var openCmd = &cobra.Command{
	Use:   "open",
	Short: "Opens a session or module on a bgpmond server.",
	Long:  "Opens a session or module on a bgpmond server.",
}

// openCmd issues an OpenSession request to the bgpmond RPC server. That sessions
// should be of an available type that the server supports and it can be named however
// the client wishes. Once a session is opened it should be closed by the client.
var openSessionCmd = &cobra.Command{
	Use:   "session TYPE",
	Short: "Opens a new database session from the bgpmond to an available database and returns its ID.",
	Long: `Tries to open a available session with a specific type from the bgpmond,
and if successful returns the newly allocated ID for that session.`,
	Args: cobra.ExactArgs(1),
	Run:  openSession,
}

// openSession isues a request to the bgpmond server to start a new sessions from the ones it has available.
// it ignores the first argument but needs to have that prototype as it's passed as a cobra.Command.Run function.
func openSession(_ *cobra.Command, args []string) {
	sessType := args[0]

	fmt.Println("Trying to open a available session named:", sessType, " with ID:", sID)
	bc, clierr := newBgpmonCli(bgpmondHost, bgpmondPort)
	if clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
		return
	}
	defer bc.close()
	emsg := &pb.OpenSessionRequest{
		SessionName: sessType,
		SessionId:   sID,
		Workers:     nw,
	}
	ctx, cancel := getCtxWithCancel()
	defer cancel()
	reply, err := bc.cli.OpenSession(ctx, emsg)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	fmt.Printf("Opened Session:%s\n", reply.SessionId)
}

// These will be the options for the openModule command
var opts string

var openModuleCmd = &cobra.Command{
	Use:   "module TYPE ID",
	Short: "Runs a module on a bgpmond server.",
	Long:  "Runs a module registered with TYPE on a bgpmond server with ID ID",
	Args:  cobra.MinimumNArgs(2),
	Run:   openModule,
}

func openModule(_ *cobra.Command, args []string) {
	modType := args[0]
	modID := args[1]

	fmt.Printf("Trying to open a module named: %s with ID: %s\n", modType, modID)
	bc, clierr := newBgpmonCli(bgpmondHost, bgpmondPort)
	if clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
		return
	}
	defer bc.close()
	emsg := &pb.RunModuleRequest{
		Type: modType,
		Id:   modID,
		Args: opts,
	}
	ctx, cancel := getCtxWithCancel()
	defer cancel()
	reply, err := bc.cli.RunModule(ctx, emsg)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	fmt.Printf("Opened module:%s\n", reply.Id)
}

func init() {
	openSessionCmd.Flags().StringVarP(&sID, "sessionId", "s", genUUID(), "UUID for the session")
	openSessionCmd.Flags().Uint32VarP(&nw, "workers", "w", 0, "Number of maximum concurrent workers (default uses the server provided value)")

	openModuleCmd.Flags().StringVarP(&opts, "opts", "o", "", "options for the module")
	openCmd.AddCommand(openSessionCmd)
	openCmd.AddCommand(openModuleCmd)
	rootCmd.AddCommand(openCmd)
}

// genUUID cretes a new UUID that will be the name of the new session.
func genUUID() string {
	return uuid.New().String()
}
