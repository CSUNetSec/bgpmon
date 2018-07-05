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
	Use:   "open",
	Short: "opens a new database session from the bgpmond and returns its ID",
	Long: `Tries to open a configured session with a specific name from the bgpmond,
and if succesful returns the newly allocated ID for that session`,
	Run: openSess,
}

func openSess(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println("Error: requiring session name to open")
		return
	}
	fmt.Println("Trying to open a configured session named:", args[0], " with ID:", sid)
	if bc, clierr := NewBgpmonCli(bgpmondHost, bgpmondPort); clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
	} else {
		defer bc.Close()
		emsg := &pb.OpenSessionRequest{
			SessionName: args[0],
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
	openCmd.Flags().Uint32VarP(&nw, "workers", "w", 1, "number of maximum concurrent workers")
}

func genuuid() string {
	return uuid.New().String()
}
