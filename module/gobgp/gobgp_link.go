package gobgp

import (
    "fmt"
    "io"
    "strconv"
    "strings"
    "time"

    "github.com/CSUNetSec/bgpmon/module"
	pb "github.com/CSUNetSec/bgpmon/protobuf"
    "github.com/CSUNetSec/bgpmon/session"
    gobgpAPI "github.com/osrg/gobgp/api"
    "golang.org/x/net/context"
    "google.golang.org/grpc"
)

type GoBGPLinkConfig struct {
}

type GoBGPLinkModule struct {
    address string
    conn    *grpc.ClientConn
    sessions []session.Sessioner
}

func NewGoBGPLinkModule(address string, sessions []session.Sessioner, config GoBGPLinkConfig) (*module.Module, error) {
    //connect over grpc
    conn, err := grpc.Dial(fmt.Sprintf("%s:50051", address), grpc.WithInsecure())
    if err != nil {
        return nil, err
    }

    return &module.Module{Moduler:GoBGPLinkModule{address, conn, sessions}}, nil
}

func (g GoBGPLinkModule) Run() error {
    //open gobgp stream
    gobgpClient := gobgpAPI.NewGobgpApiClient(g.conn)
    bgpStream, err := gobgpClient.MonitorBestChanged(context.Background(), &gobgpAPI.Arguments{})
    if err != nil {
        return err
    }

    //start new go routine to listen for messages over bgp stream
    go func() {
        for {
            destination, err := bgpStream.Recv()
            if err == io.EOF {
                break
            } else if err != nil {
                return
            }

            //parse fields from prefix
            prefixFields := strings.Split(destination.Prefix, "/")
            prefixMask, err := strconv.ParseUint(prefixFields[1], 10, 32)
			if err != nil {
				return
			}

            //create IPPrefix protobuf message
            ipPrefix := new(pb.IPPrefix)
            ipPrefix.PrefixIpAddress = prefixFields[0]
            ipPrefix.PrefixMask = uint32(prefixMask)

            //loop over paths
            for _, path := range destination.GetPaths() {
                //create BGPUpdateMessage protobuf
                bgpUpdateMessage := new(pb.BGPUpdateMessage)
                bgpUpdateMessage.Timestamp = time.Now().UTC().Unix()
                bgpUpdateMessage.CollectorIpAddress = g.address
                bgpUpdateMessage.CollectorPort = 50051
                bgpUpdateMessage.PeerIpAddress = path.NeighborIp
                bgpUpdateMessage.AsPath = append(bgpUpdateMessage.AsPath, path.SourceAsn)

                if path.IsWithdraw {
                    bgpUpdateMessage.WithdrawnRoutes = append(bgpUpdateMessage.WithdrawnRoutes, ipPrefix)
                } else {
                    bgpUpdateMessage.AdvertisedRoutes = append(bgpUpdateMessage.AdvertisedRoutes, ipPrefix)
                }

                //write to sessions
                writeRequest := new(pb.WriteRequest)
                writeRequest.Type = pb.WriteRequest_BGP_UPDATE
                writeRequest.BgpUpdateMessage = bgpUpdateMessage
                for _, session := range g.sessions {
                    session.Write(writeRequest)
                }
            }
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
