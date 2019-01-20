package cmd

import (
	"fmt"
	monpb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/CSUNetSec/protoparse/fileutil"
	"github.com/CSUNetSec/protoparse/filter"

	"github.com/spf13/cobra"
	"io"
)

var (
	mrtFile    string
	filterFile string
	sessId     *string
)

// writeCmd represents the write command
var writeCmd = &cobra.Command{
	Use:   "write SESS_ID",
	Short: "writes messages to a session ID",
	Long: `write will write BGP captures to the open db session identified by SESS_ID
Depending on the flags it can read captures from an MRT file, an running gobgpd instance, or from its stdin`,
	Args: cobra.ExactArgs(1),
	Run:  writeFunc,
}

func writeFunc(cmd *cobra.Command, args []string) {
	var (
		filts  []filter.Filter
		sessId = args[0]
	)

	if bc, clierr := NewBgpmonCli(bgpmondHost, bgpmondPort); clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
		return
	} else {
		defer bc.Close()
		ctx, cancel := getBackgroundCtxWithCancel()
		stream, err := bc.cli.Write(ctx)
		if err != nil {
			panic(err)
		}
		defer cancel() //free up context after we're done.

		if filterFile != "" {
			if filts, err = fileutil.NewFiltersFromFile(filterFile); err != nil {
				fmt.Printf("error:%s\n", err)
				return
			}
		}
		if mrtFile != "" {
			if mf, err := fileutil.NewMrtFileReader(mrtFile, filts); err != nil {
				fmt.Printf("error:%s\n", err)
				return
			} else {
				defer mf.Close()
				tot, parsed := 0, 0
				for mf.Scan() {
					pb, err := mf.GetCapture()
					tot++
					if err != nil {
						fmt.Printf("parse error:%s\n", err)
						continue
					}
					if pb != nil {
						parsed++
						writeRequest := new(monpb.WriteRequest)
						writeRequest.Type = monpb.WriteRequest_BGP_CAPTURE
						writeRequest.SessionId = sessId
						writeRequest.BgpCapture = pb
						if err := stream.Send(writeRequest); err != nil {
							fmt.Println("error in write request:%s. cancelling...", err)
							cancel()
							fmt.Println("terminating.")
							break
						}
					}
				}

				if reply, err := stream.CloseAndRecv(); err != io.EOF {
					fmt.Printf("Write stream server error:%s\n", err)
				} else {
					fmt.Printf("Write stream reply:%+v\n", reply)
				}
				if err := mf.Err(); err != nil && err != io.EOF {
					fmt.Printf("MRT file reader error:%s\n", mf.Err())
				}
				fmt.Printf("Total messages:%d \n", tot)
			}
		}
	}
}

func init() {
	rootCmd.AddCommand(writeCmd)
	writeCmd.PersistentFlags().StringVarP(&mrtFile, "mrtFile", "m", "", "the MRT file to read captures")
	writeCmd.PersistentFlags().StringVarP(&filterFile, "filterFile", "f", "", "the file to read filters from")
}
