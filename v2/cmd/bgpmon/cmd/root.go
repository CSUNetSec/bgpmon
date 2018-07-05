package cmd

import (
	"context"
	"fmt"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"google.golang.org/grpc"
	"os"
	"time"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile        string
	bgpmondHost    string
	bgpmondPort    uint32
	rpcTimeoutSecs uint32
)

type bgpmonCli struct {
	conn *grpc.ClientConn
	cli  pb.BgpmondClient
}

func NewBgpmonCli(host string, port uint32) (*bgpmonCli, error) {
	ret := &bgpmonCli{}
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", host, port), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	ret.conn = conn
	ret.cli = pb.NewBgpmondClient(conn)
	return ret, nil
}

func (b *bgpmonCli) Close() {
	b.conn.Close()
}

// helper func to return a context with a cancelfunc with timeout
func getCtxWithCancel() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Duration(rpcTimeoutSecs)*time.Second)
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
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/.bgpmon.yaml)")
	rootCmd.PersistentFlags().StringVarP(&bgpmondHost, "host", "H", "127.0.0.1", "bgpmond host to connect to (default is 127.0.0.1)")
	rootCmd.PersistentFlags().Uint32VarP(&bgpmondPort, "port", "P", 12289, "bgpmond port to connect to (default is 6060)")
	rootCmd.PersistentFlags().Uint32VarP(&rpcTimeoutSecs, "rpcTimeout", "t", 5, "seconds in which an RPC request should timeout")
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

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
