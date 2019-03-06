[![GoDoc](https://godoc.org/github.com/CSUNetSec/bgpmon?status.svg)](https://godoc.org/github.com/CSUNetSec/bgpmon)
[![Build Status](https://travis-ci.org/CSUNetSec/bgpmon.svg?branch=master)](https://travis-ci.org/CSUNetSec/bgpmon)
[![Go Report Card](https://goreportcard.com/badge/github.com/CSUNetSec/bgpmon)](https://goreportcard.com/report/github.com/CSUNetSec/bgpmon)

BGPmon is a client server application to store and analyze large amounts
of BGP data.

# Installation

Requirements
1. Golang > 1.11
2. make
3. to store message a db backend (Postgresql >= 9.5)

Running make will create two binaries under bin. To run the daemon,
run:

    bgpmond conf-file

To succesfully store messages in a database please have a Postgresql with a user that has access to write
create tables on a database and reflect that configuration in the config file.

# Example client commands

The client works over RPC, so the rpc module must be started in order
for the client to work.

To show configured sessions on the server

    bgpmon listAvailable

To open a configured session

    bgpmon open LocalPostgres -s sID

To write MRT files

    bgpmon write sID mrtFiles...

To close a session

    bgpmon close sID

# Example config file

    DebugOut = "stdout"
    ErrorOut = "stderr"

    # Sessions represent the possible database backends
    [Sessions]
    #this will configure an available session named LocalPostgres
    [Sessions.LocalPostgres]
    Type = "postgres"
    Hosts = ["localhost"]
    Database = "bgpmon"
    User = "bgpmon"
    Password = "bgpmon"
    WorkerCt = 4 #the maximum amount of concurrent workers
    DBTimeoutSecs = 120 #maximum lifetime seconds for a DB operation

    # Modules represent modules to run on startup
    # Multiple modules of the same type can be instantiated with
    # different IDs
    [Modules]
    # RPC exposes the basic bgpmond operation over an RPC interface that
    # consumes and produces protocol buffers defined in the netsec-protobufs
    # repository /bgpmon
    [Modules.rpc1]
    Type="rpc"
    Args="-address :12289 -timeoutsecs 240"

    # pprof enables the http profiler at an address specified by Args
    [Modules.pprof1]
    Type="pprof"
    Args="-address localhost:6969"

    # Nodes represent operator provided information for nodes involved in
    # BGP transactions
    # If there are already saved nodes in the database that conflict with the
    # configuration, the configuration is preferred
    [Nodes]
    [Nodes."128.223.51.102"]
    Name="routeviews2"
    IsCollector=true
    DumpDurationMinutes=1440
    Descritption="routeviews.org routeviews2 collector"
