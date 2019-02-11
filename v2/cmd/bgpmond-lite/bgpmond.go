package main

import (
	"fmt"
	"os"
	"time"

	"github.com/CSUNetSec/bgpmon/v2/config"
	"github.com/CSUNetSec/bgpmon/v2/core"
	_ "github.com/CSUNetSec/bgpmon/v2/modules"
	"github.com/CSUNetSec/bgpmon/v2/util"

	"net/http"
	_ "net/http/pprof"
)

var mainlogger = util.NewLogger("system", "main")

func main() {
	if len(os.Args) != 2 {
		mainlogger.Fatalf("no configuration file provided")
	}

	mainlogger.Infof("Reading config file: %s", os.Args[1])
	if cfile, ferr := os.Open(os.Args[1]); ferr != nil {
		mainlogger.Fatalf("error opening configuration file:%s", ferr)
	} else if bc, cerr := config.NewConfig(cfile); cerr != nil {
		mainlogger.Fatalf("configuration error:%s", cerr)
	} else {
		cfile.Close()
		daemonConf := bc.GetDaemonConfig()
		if daemonConf.ProfilerOn {
			mainlogger.Infof("Starting pprof at address:%s", daemonConf.ProfilerHostPort)
			go func(addr string, log util.Logger) {
				log.Fatalf("%v", http.ListenAndServe(addr, nil))
			}(daemonConf.ProfilerHostPort, util.NewLogger("system", "pprof"))
		}

		server := core.NewServer(bc)
		printArray(server.ListModuleTypes())
		err := server.RunModule("example_daemon", "ID1", "")
		panicIfNotNil(err)
		err = server.RunModule("example_task", "ID2", "")
		time.Sleep(2 * time.Second)
		printArray(server.ListRunningModules())
		server.CloseModule("ID1")
		time.Sleep(2 * time.Second)
		printArray(server.ListRunningModules())
	}
}

func printArray(arr []string) {
	for _, s := range arr {
		fmt.Printf("%s\n", s)
	}
}

func panicIfNotNil(err error) {
	if err != nil {
		panic(err)
	}
}
