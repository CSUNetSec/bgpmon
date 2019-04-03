// The bgpmond command launches the bgpmon server with a provided configuration file.
// It launches a default RPC module if one isn't provided in the configuration.
package main

import (
	"fmt"
	"os"
	"os/signal"

	core "github.com/CSUNetSec/bgpmon"
	"github.com/CSUNetSec/bgpmon/config"
	_ "github.com/CSUNetSec/bgpmon/modules"
	"github.com/CSUNetSec/bgpmon/util"
)

// mainLogger is the standard logging struct for
// the main struct.
var mainLogger = util.NewLogger("system", "main")

// Launches the BgpmondServer with a configuration file provided on the command
// line. If the config file provided doesn't contain an RPC module, this launches
// a default RPC. This command will only halt on ctrl-C, at which point it will
// shut down the server.
func main() {
	if len(os.Args) != 2 {
		mainLogger.Fatalf("No configuration file provided")
	}

	server, err := core.NewServerFromFile(os.Args[1])
	if err != nil {
		mainLogger.Fatalf("Error creating server: %s", err)
	}

	rpcRunning := false
	for _, v := range server.ListRunningModules() {
		if v.Type == "rpc" {
			rpcRunning = true
			break
		}
	}

	if !rpcRunning {
		mainLogger.Infof("Configuration didn't include RPC module, launching default")
		rpcOpts, err := util.StringToOptMap(fmt.Sprintf("-address %s -timeoutsecs %d", config.DefaultRPCAddress, config.DefaultRPCTimeoutSecs))
		if err != nil {
			mainLogger.Fatalf("Error parsing default RPC options: %s", err)
		}
		err = server.RunModule("rpc", "rpc", rpcOpts)
		if err != nil {
			mainLogger.Fatalf("Error starting RPC module: %s", err)
		}
	}

	waitOnInterrupt()
	mainLogger.Infof("Received SIGINT, shutting down")

	if err := server.Close(); err != nil {
		mainLogger.Fatalf("Error shutting down server: %s", err)
	}
}

func waitOnInterrupt() {
	close := make(chan os.Signal, 1)
	signal.Notify(close, os.Interrupt)
	<-close
	return
}
