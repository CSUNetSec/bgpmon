package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/CSUNetSec/bgpmon/v2/core"
	_ "github.com/CSUNetSec/bgpmon/v2/modules"
	"github.com/CSUNetSec/bgpmon/v2/util"
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

	err = server.RunModule("rpc", "rpc", ":12289")
	if err != nil {
		mainlogger.Fatalf("Failed to start RPC module: %s", err)
	}

	err = server.RunModule("pprof", "pprof", "localhost:6969")
	if err != nil {
		mainlogger.Errorf("Failed to start pprof module: %s", err)
	}

	WaitOnInterrupt()
	mainlogger.Infof("Recieved SIGINT, shutting down")
	server.Close()
}

func WaitOnInterrupt() {
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
