package cmd

import (
	"fmt"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

var (
	sid string //id for for the open session request
	nw  uint32 //number of maximum database workers
)

// openCmd represents the open command
var openCmd = &cobra.Command{
	Use:   "open SESSION_TYPE",
	Short: "opens a new database session from the bgpmond and returns its ID",
	Long: `Tries to open a available session with a specific type from the bgpmond,
and if succesful returns the newly allocated ID for that session`,
	Args: cobra.ExactArgs(1),
	Run:  openSess,
}

func openSess(cmd *cobra.Command, args []string) {
	sessType := args[0]

	fmt.Println("Trying to open a available session named:", sessType, " with ID:", sid)
	if bc, clierr := NewBgpmonCli(bgpmondHost, bgpmondPort); clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
	} else {
		defer bc.Close()
		emsg := &pb.OpenSessionRequest{
			SessionName: sessType,
			SessionId:   sid,
			Workers:     nw,
		}
		ctx, cancel := getCtxWithCancel()
		defer cancel()
		if reply, err := bc.cli.OpenSession(ctx, emsg); err != nil {
			fmt.Printf("Error: %s\n", err)
		} else {
			fmt.Printf("Opened Session:%s\n", reply.SessionId)
		}
	}
}

func init() {
	rootCmd.AddCommand(openCmd)
	openCmd.Flags().StringVarP(&sid, "sessionId", "s", genuuid(), "UUID for the session")
	openCmd.Flags().Uint32VarP(&nw, "workers", "w", 0, "number of maximum concurrent workers")
}

func genuuid() string {
	return uuid.New().String()
}
