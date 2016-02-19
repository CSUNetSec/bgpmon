package main

import (
	"errors"
	"flag"
	"fmt"
	"net"

	"github.com/hamersaw/bgpmon/log"
	"github.com/hamersaw/bgpmon/module"
	//"github.com/hamersaw/bgpmon/module/bgp"
	//"github.com/hamersaw/bgpmon/module/gobgp"
	pb "github.com/hamersaw/bgpmon/proto/bgpmond"
	"github.com/hamersaw/bgpmon/session"

	"github.com/google/uuid"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var debugOut, errorOut, ipAddress string
var port int

func init() {
	flag.StringVar(&debugOut, "debug_out", "discard", "debug output stream")
	flag.StringVar(&errorOut, "error_out", "discard", "error output stream")
	flag.StringVar(&ipAddress, "ip_address", "", "IP Address for GRPC server")
	flag.IntVar(&port, "port", 12289, "Port for GRPC server")
}

func main() {
	flag.Parse()

	debugClose, errorClose, err := log.Init(debugOut, errorOut)
	if err != nil {
		panic(err)
	}
	defer debugClose()
	defer errorClose()

	listen, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ipAddress, port))
	if err != nil {
		panic(err)
	}

	bgpmondServer := Server {
		sessions: make(map[string]session.Session),
		modules: make(map[string]module.Moduler),
	}

	grpcServer := grpc.NewServer()
	pb.RegisterBgpmondServer(grpcServer, bgpmondServer)
	grpcServer.Serve(listen)
}

type Server struct {
	sessions map[string]session.Session //map from uuid to session interface
	modules  map[string]module.Moduler  //map from uuid to running module interface
}

func (s Server) CloseSession(ctx context.Context, config *pb.CloseSessionConfig) (result *pb.CloseSessionResult, err error) {
	err = errors.New("unimplemented")
	return
}

func (s Server) OpenSession(ctx context.Context, config *pb.OpenSessionConfig) (*pb.OpenSessionResult, error) {
	result := new(pb.OpenSessionResult)
	sessionID := newID()
	switch config.Type {
	case pb.OpenSessionConfig_CASSANDRA:
		casConfig := config.GetCassandraSession()
		casSession, err := session.NewCassandraSession(casConfig.Username, casConfig.Password, casConfig.Hosts)
		if err != nil {
			result.Success = false
			result.ErrorMessage = fmt.Sprintf("%v", err)
			break
		}

		s.sessions[sessionID] = casSession

		result.Success = true;
		result.SessionId = sessionID
	case pb.OpenSessionConfig_FILE:
		fileConfig := config.GetFileSession()
		fileSession, err := session.NewFileSession(fileConfig.Filename)
		if err != nil {
			result.Success = false
			result.ErrorMessage = fmt.Sprintf("%v", err)
			break
		}

		s.sessions[sessionID] = fileSession

		result.Success = true;
		result.SessionId = sessionID
	default:
		result.Success = false;
		result.ErrorMessage = "unimplemented session type"
	}

	return result, nil
}

func newID() string {
	return uuid.New()
}
