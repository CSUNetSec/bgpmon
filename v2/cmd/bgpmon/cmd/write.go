package cmd

import (
	"fmt"
	"github.com/CSUNetSec/protoparse/fileutil"
	"github.com/CSUNetSec/protoparse/filter"

	"github.com/spf13/cobra"
)

var (
	mrtFile    string
	filterFile string
)

// writeCmd represents the write command
var writeCmd = &cobra.Command{
	Use:   "write",
	Short: "writes messages to a session ID",
	Long: `write will write BGP captures to the open db session identified by ID
Depending on the flags it can read captures from an MRT file, an running gobgpd instance, or from its stdin`,
	Run: writeFunc,
}

func writeFunc(cmd *cobra.Command, args []string) {
	var (
		filts []filter.Filter
		err   error
	)
	if len(args) != 1 {
		fmt.Printf("Error: write requires a session ID\n")
	}
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
			for pb, err := mf.Scan(); mf.Error() == nil; {
				tot++
				if err != nil {
					fmt.Printf("parse error:%s\n", err)
					continue
				}
				if pb != nil {
					parsed++
					fmt.Printf("read entry %d\n", parsed)
				}
			}
			fmt.Printf("Total:%d\n", tot)
		}
	}
}

func init() {
	rootCmd.AddCommand(writeCmd)
	writeCmd.PersistentFlags().StringVarP(&mrtFile, "mrtFile", "m", "", "the MRT file to read captures")
	writeCmd.PersistentFlags().StringVarP(&filterFile, "filterFile", "f", "", "the file to read filters from")
}
