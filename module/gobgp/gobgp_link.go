package gobgp

import (
    "fmt"
    "io"

	"github.com/CSUNetSec/bgpmon/log"
    "github.com/CSUNetSec/bgpmon/module"
    "github.com/CSUNetSec/bgpmon/session"
    gobgpAPI "github.com/osrg/gobgp/api"
    "golang.org/x/net/context"
    "google.golang.org/grpc"
)

type GoBGPLinkConfig struct {
}

type GoBGPLinkModule struct {
    conn *grpc.ClientConn
}

func NewGoBGPLinkModule(address string, sessions []session.Sessioner, config GoBGPLinkConfig) (*module.Module, error) {
    log.Debl.Printf("starting gobgp link module with arguments: %s\n", address)
    //connect over grpc
    conn, err := grpc.Dial(fmt.Sprintf("%s:50051", address), grpc.WithInsecure())
    if err != nil {
        return nil, err
    }

    return &module.Module{Moduler:GoBGPLinkModule{conn}}, nil
}

func (g GoBGPLinkModule) Run() error {
    //open gobgp stream
    gobgpClient := gobgpAPI.NewGobgpApiClient(g.conn)
    bgpStream, err := gobgpClient.MonitorBestChanged(context.Background(), &gobgpAPI.Arguments{})
    if err != nil {
        return err
    }

    //start new go routing to listen for messages over bgp stream
    go func() {
        for {
            message, err := bgpStream.Recv()
            if err == io.EOF {
                break
            } else if err != nil {
                return
            }

            fmt.Printf("%+v\n", message)
            //TODO turn this into bgp protobuf and write to cassandra
        }
    }()

    return nil
}

func (g GoBGPLinkModule) Status() string {
	return ""
}

func (g GoBGPLinkModule) Cleanup() error {
    err := g.conn.Close()

	return err
}

