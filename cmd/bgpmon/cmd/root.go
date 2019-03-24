// Package cmd is the package that provides implementations for the bgpmon client
// commands. It utilizes the cobra and viper configuration frameworks which
// make it possible for commands to either be specified by command line options
// or configuration files.
package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

var (
	cfgFile        string
	bgpmondHost    string
	bgpmondPort    uint32
	rpcTimeoutSecs uint32
)

// bgpmonCli is a structure that unifies a grpc connection with the bgpmond
// client protobuf specification from the netsec-protobufs repository.
type bgpmonCli struct {
	conn *grpc.ClientConn
	cli  pb.BgpmondClient
}

func newBgpmonCli(host string, port uint32) (*bgpmonCli, error) {
	ret := &bgpmonCli{}
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", host, port), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	ret.conn = conn
	ret.cli = pb.NewBgpmondClient(conn)
	return ret, nil
}

func (b *bgpmonCli) close() {
	if err := b.conn.Close(); err != nil {
		fmt.Printf("Error:%s while closing the connection to bgpmond", err)
	}
}

// helper func to return a context with a cancelfunc with timeout.
func getCtxWithCancel() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Duration(rpcTimeoutSecs)*time.Second)
}

// getBackgroundCtxWithCancel derives a new context for the background one, alongside with a cancel function
// that can be invoked by a holder. The purpose of this context is to pass cancellation request from the RPC
// client events to the server components that can be running on another host.
func getBackgroundCtxWithCancel() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

var rootCmd = &cobra.Command{
	Use:   "bgpmon",
	Short: "A command line client to interface with bgpmon",
	Long: `bgpmon connects to a running bgpmon daemon and communicates
with it by issuing commands over RPC.`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "C", "$HOME/.bgpmon.yaml", "Config file")
	rootCmd.PersistentFlags().StringVarP(&bgpmondHost, "host", "H", "127.0.0.1", "bgpmond host to connect to")
	rootCmd.PersistentFlags().Uint32VarP(&bgpmondPort, "port", "P", 6060, "bgpmond port to connect to")
	rootCmd.PersistentFlags().Uint32VarP(&rpcTimeoutSecs, "rpcTimeout", "T", 5, "Seconds in which an RPC request should timeout")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".bgpmon" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".bgpmon")
	}

	viper.AutomaticEnv() // Read in environment variables that match.

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	} else if !os.IsNotExist(err) { // The config file exists but it errored in parsing.
		fmt.Printf("Error:%s in parsing config file:%s\n", viper.ConfigFileUsed(), err)
		os.Exit(1)
	}
}
