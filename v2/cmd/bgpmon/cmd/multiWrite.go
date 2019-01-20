package cmd

import (
	"fmt"
	"github.com/CSUNetSec/bgpmon/v2/util"
	monpb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/CSUNetSec/protoparse/fileutil"
	//	"github.com/CSUNetSec/protoparse/filter"
	"io"
	"sync"

	"github.com/spf13/cobra"
)

var multiWriteCmd = &cobra.Command{
	Use:   "multiWrite SESS_ID <files>",
	Short: "writes a series of files to a session ID",
	Long:  "Given a series of files, multiWrite will write up to <worker count> of them at a single time, reporting the individual and total success upon completion.",
	Args:  cobra.MinimumNArgs(2),
	Run:   multiWriteFunc,
}

var wc int

type writeMRTResult struct {
	fname string
	wCt   int
	err   error
}

func multiWriteFunc(cmd *cobra.Command, args []string) {
	sessId := args[0]

	bc, clierr := NewBgpmonCli(bgpmondHost, bgpmondPort)
	if clierr != nil {
		fmt.Printf("Error: %s\n", clierr)
		return
	}
	defer bc.Close()

	results := make(chan writeMRTResult)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go summarizeResults(results, wg)

	// This should be a number received from the daemon
	wp := util.NewWorkerPool(wc)
	for _, fname := range args[1:] {
		wp.Add()
		go func() {
			ct, err := writeMRTFile(bc, fname, sessId)
			if err == io.EOF { //do not consider that an error
				err = nil
			}
			results <- writeMRTResult{fname: fname, wCt: ct, err: err}
			wp.Done()
		}()
	}
	wp.Close()
	close(results)
	wg.Wait()
}

func summarizeResults(in chan writeMRTResult, wg *sync.WaitGroup) {
	defer wg.Done()

	closed := false
	tot := 0
	failCt := 0
	// These arrays should stay in sync
	var failed []string
	var reasons []error

	for !closed {
		select {
		case r, ok := <-in:
			if !ok {
				closed = true
				break
			}

			tot++
			if r.err != nil {
				failCt++
				failed = append(failed, r.fname)
				reasons = append(reasons, r.err)
			}
		}
	}

	fmt.Printf("Total completed: %d\n", tot)
	fmt.Printf("Total failures: %d\n", failCt)
	if failCt > 0 {
		for ct := range failed {
			fmt.Printf("%s : %s\n", failed[ct], reasons[ct])
		}
	}
}

func writeMRTFile(bc *bgpmonCli, fname, sessId string) (int, error) {
	ctx, cancel := getBackgroundCtxWithCancel()
	stream, err := bc.cli.Write(ctx)
	if err != nil {
		return 0, err
	}
	defer cancel()

	mf, err := fileutil.NewMrtFileReader(fname, nil)
	if err != nil {
		return 0, err
	}

	defer mf.Close()
	parsed := 0
	for mf.Scan() {
		pb, err := mf.GetCapture()
		if err != nil {
			fmt.Printf("Parse error: %s\n", err)
			continue
		}
		if pb != nil {
			parsed++
			writeRequest := new(monpb.WriteRequest)
			writeRequest.Type = monpb.WriteRequest_BGP_CAPTURE
			writeRequest.SessionId = sessId
			writeRequest.BgpCapture = pb
			if err := stream.Send(writeRequest); err != nil {
				cancel()
				return 0, err
			}
		}
	}

	if _, err := stream.CloseAndRecv(); err != io.EOF {
		return parsed, fmt.Errorf("Write stream server error: %s", err)
	} else if err := mf.Err(); err != nil {
		return parsed, fmt.Errorf("MRT file reader error: %s", err)
	}
	return parsed, nil
}

func init() {
	rootCmd.AddCommand(multiWriteCmd)
	multiWriteCmd.Flags().IntVarP(&wc, "workers", "w", 1, "Override the number of workers writing files")
}
