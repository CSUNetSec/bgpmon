package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"

	core "github.com/CSUNetSec/bgpmon"
	_ "github.com/CSUNetSec/bgpmon/modules"
	"github.com/CSUNetSec/bgpmon/util"
)

var mainlogger = util.NewLogger("system", "main")

func main() {
	if len(os.Args) != 2 {
		mainlogger.Fatalf("No configuration file provided")
	}

	server, err := core.NewServerFromFile(os.Args[1])
	if err != nil {
		mainlogger.Fatalf("Error creating server: %s", err)
	}

	rpcRunning := false
	for _, v := range server.ListRunningModules() {
		if strings.Contains(v, "ID: rpc") {
			rpcRunning = true
			break
		}
	}

	if !rpcRunning {
		mainlogger.Infof("Configuration didn't include RPC module, launching default")
		err = server.RunModule("rpc", "rpc", ":12289")
		if err != nil {
			mainlogger.Fatalf("Error starting RPC module: %s", err)
		}
	}

	waitOnInterrupt()
	mainlogger.Infof("Recieved SIGINT, shutting down")
	server.Close()
}

func waitOnInterrupt() {
	close := make(chan os.Signal, 1)
	signal.Notify(close, os.Interrupt)
	<-close
	return
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
