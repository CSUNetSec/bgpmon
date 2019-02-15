BGPmon is a client server application to store and analyze large amounts
of BGP data.

#Installation
Requirements
1. Golang > 1.11
2. make

Running make will create two binaries under bin. To run the daemon,
run:
    bgpmond conf-file

#Example client commands
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

#Example config file
    DebugOut = "stdout"
    ErrorOut = "stderr"

    # Sessions represent the possible database backends
    [Sessions]
    [Sessions.LocalPostgres]
    Type = "postgres"
    Hosts = ["localhost"]
    Database = "bgpmon"
    User = "bgpmon"
    Password = "bgpmon"
    WorkerCt = 4

    # Modules represent modules to run on startup
    # Multiple modules of the same type can be instantiated with
    # different IDs
    [Modules]
    # RPC exposes the basic bgpmond operation over an RPC interface that
    # consumes and produces protocol buffers defined in the netsec-protobufs
    # repository /bgpmon
    [Modules.rpc]
    Type="rpc"
    ID="rpc"
    Args=":12289"

    # pprof enables the http profiler at an address specified by Args
    [Modules.pprof]
    Type="pprof"
    ID="pprof"
    Args="localhost:6969"

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
