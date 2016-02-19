package main

import (
	"fmt"
	"github.com/codegangsta/cli"
	"google.golang.org/grpc"
	"os"
)

/*
bgpmon connect
	cassandra
	file

bgpmon desc
	conns
	modules

bgpmon disconnect
	<connection-id>

bgpmon list
	conns
	modules

bgpmon start
	module

bgpmon status
	bgpmond
	module

bgpmon stop 
	module

bgpmon write
	as-location
	mrt-file
	ip-location
	prefix-location
*/

var ipAddress, port string

func main() {
	app := cli.NewApp()
	app.Name = "bgpmon"
	app.Usage = "provide an easy user interface with bgpmond"
	app.Version = "0.0.1"

	app.Flags = []cli.Flag {
		cli.StringFlag {
			Name:        "ipAddress",
			Value:       "127.0.0.1",
			Usage:       "IP Address of bgpmond connection",
			Destination: &ipAddress,
		},
		cli.StringFlag {
			Name:        "port",
			Value:       "12289",
			Usage:       "Port of bgpmond connection",
			Destination: &port,
		},
	}

	app.Commands = []cli.Command {
		{
			Name:        "connect",
			Usage:       "options for handling bgpmond backend connections",
			Subcommands: []cli.Command {
				{
					Name:   "cassandra",
					Usage:  "cassandra <username> <password> <hosts>",
					Action: connCassandra,
				},
				{
					Name:   "file",
					Usage:  "file <filename>",
					Action: connFile,
				},
			},
		},
		{
			Name:        "desc",
			Usage:       "describe available options for bgpmond components",
			Subcommands: []cli.Command {
				{
					Name:   "conns",
					Usage:  "describe available connection types",
					Action: descConns,
				},
				{
					Name:   "modules",
					Usage:  "describe availabe modules",
					Action: descModules,
				},
			},
		},
		{
			Name:   "disconnect",
			Usage:  "disconnect a bgpmond connection",
			Action: disconnect,
		},
		{
			Name:        "list",
			Usage:       "list availabe bgpmond componenets",
			Subcommands: []cli.Command {
				{
					Name:   "conns",
					Usage:  "list open connections from bgpmond",
					Action: listConns,
				},
				{
					Name:   "modules",
					Usage:  "list running modules from bgpmond",
					Action: listModules,
				},
			},
		},
		{
			Name:        "start",
			Usage:       "start a component of bgpmond",
			Subcommands: []cli.Command {
				{
					Name:   "module",
					Usage:  "start a module on bgpmond",
					Action: startModule,
				},
			},
		},
		{
			Name:        "status",
			Usage:       "get the status of a component of bgpmond",
			Subcommands: []cli.Command {
				{
					Name:   "bgpmond",
					Usage:  "get the status of the bgpmon daemon",
					Action: statusBgpmond,
				},
				{
					Name:   "module",
					Usage:  "get the status of a module of bgpmond",
					Action: statusModule,
				},
			},
		},
		{
			Name:        "stop",
			Usage:       "stop a component of bgpmond",
			Subcommands: []cli.Command {
				{
					Name:   "module",
					Usage:  "stop a module on bgpmond",
					Action: stopModule,
				},
			},
		},
		{
			Name:        "write",
			Usage:       "write data to a bgpmond connection",
			Subcommands: []cli.Command {
				{
					Name:   "as-location",
					Usage:  "write as location data to a bgpmond connection",
					Action: writeASLocation,
				},
				{
					Name:   "mrt-file",
					Usage:  "write an mrt file to a bgpmond connection",
					Action: writeMRTFile,
				},
				{
					Name:   "ip-location",
					Usage:  "write ip address location data to a bgpmond connection",
					Action: writeIPLocation,
				},
				{
					Name:   "prefix-location",
					Usage:  "write prefix location data to a bgpmond connection",
					Action: writePrefixLocation,
				},
			},
		},
	}

	app.Run(os.Args)
}

func connectRPC() (*grpc.ClientConn, error) {
	return grpc.Dial(fmt.Sprintf("%s:%s", ipAddress, port), grpc.WithInsecure())
}
