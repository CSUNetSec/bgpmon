package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/CSUNetSec/bgpmon/log"
	"github.com/CSUNetSec/bgpmon/module"
	"github.com/CSUNetSec/bgpmon/module/bgp"
	"github.com/CSUNetSec/bgpmon/module/gobgp"
	"github.com/CSUNetSec/bgpmon/session"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon"

	"github.com/BurntSushi/toml"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var bgpmondConfig BgpmondConfig

type BgpmondConfig struct {
	Address  string
	DebugOut string
	ErrorOut string
	Modules  ModuleConfig
	Sessions SessionConfig
}

type ModuleConfig struct {
	GoBGPLink        gobgp.GoBGPLinkConfig
	PrefixByAsNumber bgp.PrefixByAsNumberConfig
	PrefixHijack     bgp.PrefixHijackConfig
}

type SessionConfig struct {
	Cassandra session.CassandraConfig
	File      session.FileConfig
	Cockroach session.CockroachConfig
}

func main() {
	if _, err := toml.DecodeFile(os.Args[1], &bgpmondConfig); err != nil {
		panic(err)
	}

	debugClose, errorClose, err := log.Init(bgpmondConfig.DebugOut, bgpmondConfig.ErrorOut)
	if err != nil {
		panic(err)
	}
	defer debugClose()
	defer errorClose()

	listen, err := net.Listen("tcp", bgpmondConfig.Address)
	if err != nil {
		panic(err)
	}

	bgpmondServer := Server{
		sessions: make(map[string]session.Sessioner),
		modules:  make(map[string]*module.Module),
	}

	grpcServer := grpc.NewServer()
	pb.RegisterBgpmondServer(grpcServer, bgpmondServer)
	grpcServer.Serve(listen)
}

type Server struct {
	sessions map[string]session.Sessioner //map from uuid to session interface
	modules  map[string]*module.Module    //map from uuid to running module interface
}

/*
 * Module RPC Calls
 */
func (s Server) RunModule(ctx context.Context, request *pb.RunModuleRequest) (*pb.RunModuleReply, error) {
	var mod *module.Module
	var err error

	switch request.Type {
	case pb.ModuleType_PREFIX_HIJACK:
		mod, err = s.createModule("", request.GetPrefixHijackModule())
		if err != nil {
			break
		}
	case pb.ModuleType_PREFIX_BY_AS_NUMBER:
		mod, err = s.createModule("", request.GetPrefixByAsNumberModule())
		if err != nil {
			break
		}
	default:
		return nil, errors.New("Unimplemented module type")
	}

	if err != nil {
		return nil, err
	}

	mod.CommandChan <- module.ModuleCommand{module.COMRUN, nil}
	//TODO need to kill after execution ends
	return &pb.RunModuleReply{mod.Status()}, nil
}

func (s Server) StartModule(ctx context.Context, request *pb.StartModuleRequest) (*pb.StartModuleReply, error) {
	log.Debl.Printf("Starting module %s\n", request.ModuleId)
	if _, exists := s.modules[request.ModuleId]; exists {
		return nil, errors.New(fmt.Sprintf("Module ID %s already exists", request.ModuleId))
	}

	var mod *module.Module
	var err error

	switch request.Type {
	case pb.ModuleType_GOBGP_LINK:
		mod, err = s.createModule(request.ModuleId, request.GetGobgpLinkModule())
		if err != nil {
			break
		}

		mod.CommandChan <- module.ModuleCommand{module.COMRUN, nil}
	case pb.ModuleType_PREFIX_HIJACK:
		rpcConfig := request.GetPrefixHijackModule()
		mod, err = s.createModule(request.ModuleId, rpcConfig)
		if err != nil {
			break
		}

		mod.SchedulePeriodic(rpcConfig.PeriodicSeconds, rpcConfig.TimeoutSeconds)
	default:
		return nil, errors.New("Unimplemented module type")
	}

	if err != nil {
		return nil, err
	}

	s.modules[request.ModuleId] = mod
	log.Debl.Printf("Module %s Started\n", request.ModuleId)
	return &pb.StartModuleReply{request.ModuleId}, nil
}

func (s Server) ListModules(ctx context.Context, request *pb.Empty) (*pb.ListModulesReply, error) {
	moduleIDs := []string{}
	for moduleID, _ := range s.modules {
		moduleIDs = append(moduleIDs, moduleID)
	}

	return &pb.ListModulesReply{moduleIDs}, nil
}

func (s Server) StopModule(ctx context.Context, request *pb.StopModuleRequest) (*pb.Empty, error) {
	log.Debl.Printf("Stopping module %s\n", request.ModuleId)

	mod, exists := s.modules[request.ModuleId]
	if !exists {
		return nil, errors.New(fmt.Sprintf("Module ID %s not found", request.ModuleId))
	} else {
		mod.CommandChan <- module.ModuleCommand{module.COMDIE, nil}
		delete(s.modules, request.ModuleId)
	}

	log.Debl.Printf("Module %s stopped\n", request.ModuleId)
	return &pb.Empty{}, nil
}

/*
 * Session RPC Calls
 */
func (s Server) CloseSession(ctx context.Context, request *pb.CloseSessionRequest) (*pb.Empty, error) {
	log.Debl.Printf("Closing session %s\n", request.SessionId)
	sess, exists := s.sessions[request.SessionId]
	if !exists {
		return nil, errors.New(fmt.Sprintf("Session ID %s not found", request.SessionId))
	} else {
		sess.Close()
		delete(s.sessions, request.SessionId)
	}

	log.Debl.Printf("Session %s closed\n", request.SessionId)
	return &pb.Empty{}, nil
}

func (s Server) ListSessions(ctx context.Context, request *pb.Empty) (*pb.ListSessionsReply, error) {
	sessionIDs := []string{}
	for sessionID, _ := range s.sessions {
		sessionIDs = append(sessionIDs, sessionID)
	}

	return &pb.ListSessionsReply{sessionIDs}, nil
}

func (s Server) OpenSession(ctx context.Context, request *pb.OpenSessionRequest) (*pb.OpenSessionReply, error) {
	log.Debl.Printf("Opening session %s\n", request.SessionId)
	if _, exists := s.sessions[request.SessionId]; exists {
		return nil, errors.New(fmt.Sprintf("Session ID %s already exists", request.SessionId))
	}

	var sess session.Sessioner
	var err error

	switch request.Type {
	case pb.SessionType_CASSANDRA:
		rpcConfig := request.GetCassandraSession()
		sess, err = session.NewCassandraSession(rpcConfig.Username, rpcConfig.Password, rpcConfig.Hosts, rpcConfig.WorkerCount, bgpmondConfig.Sessions.Cassandra)
		if err != nil {
			break
		}

	case pb.SessionType_COCKROACH:
		rpcConfig := request.GetCockroachSession()
		sess, err = session.NewCockroachSession(rpcConfig.Username, rpcConfig.Hosts, rpcConfig.Port, rpcConfig.WorkerCount, rpcConfig.Certdir, bgpmondConfig.Sessions.Cockroach)
		if err != nil {
			break
		}
	case pb.SessionType_FILE:
		rpcConfig := request.GetFileSession()
		sess, err = session.NewFileSession(rpcConfig.Filename, bgpmondConfig.Sessions.File)
		if err != nil {
			break
		}
	default:
		return nil, errors.New("Unimplemented session type")
	}

	if err != nil {
		return nil, err
	}

	s.sessions[request.SessionId] = sess
	log.Debl.Printf("Session %s opened\n", request.SessionId)
	return &pb.OpenSessionReply{request.SessionId}, nil
}

/*
 * Write Messages
 */
func (s Server) Write(stream pb.Bgpmond_WriteServer) error {
	for {
		writeRequest, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		sess, exists := s.sessions[writeRequest.SessionId]
		if !exists {
			fmt.Printf("session doesn't exist\n")
			//panic(errors.New(fmt.Sprintf("Session '%s' does not exists", writeRequest.SessionId)))
			return errors.New(fmt.Sprintf("Session '%s' does not exists", writeRequest.SessionId))
		}
		if err := sess.Write(writeRequest); err != nil {
			fmt.Printf("error writing\n")
			//panic(err)
			return err
		}
	}

	return nil
}

/*
 * Miscellaneous Functions
 */
func (s Server) createModule(moduleId string, request interface{}) (*module.Module, error) {
	var mod *module.Module

	switch request.(type) {
	case *pb.GoBGPLinkModule:
		rpcConfig := request.(*pb.GoBGPLinkModule)
		outSessions, err := s.getSessions(rpcConfig.OutSessionId)
		if err != nil {
			return nil, err
		}

		mod, err = gobgp.NewGoBGPLinkModule(moduleId, rpcConfig.Address, outSessions, bgpmondConfig.Modules.GoBGPLink)
		if err != nil {
			return nil, err
		}
	case *pb.PrefixByAsNumberModule:
		rpcConfig := request.(*pb.PrefixByAsNumberModule)
		inSessions, err := s.getSessions(rpcConfig.InSessionId)
		if err != nil {
			return nil, err
		}

		mod, err = bgp.NewPrefixByAsNumberModule(moduleId, rpcConfig.StartTime, rpcConfig.EndTime, inSessions, bgpmondConfig.Modules.PrefixByAsNumber)
		if err != nil {
			return nil, err
		}
	case *pb.PrefixHijackModule:
		rpcConfig := request.(*pb.PrefixHijackModule)
		inSessions, err := s.getSessions(rpcConfig.InSessionId)
		if err != nil {
			return nil, err
		}

		mod, err = bgp.NewPrefixHijackModule(moduleId, rpcConfig.PeriodicSeconds, rpcConfig.TimeoutSeconds, inSessions, bgpmondConfig.Modules.PrefixHijack,
			 rpcConfig.StartTimeSecondsFromEpoch, rpcConfig.LookbackDurationSeconds)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("Unimplemented module type")
	}

	mod.Init()
	return mod, nil
}

func (s Server) getSessions(sessionIDs []string) ([]session.Sessioner, error) {
	sessions := []session.Sessioner{}
	for _, sessionID := range sessionIDs {
		sess, exists := s.sessions[sessionID]
		if !exists {
			return nil, errors.New(fmt.Sprintf("Session '%s' does not exist", sessionID))
		}

		sessions = append(sessions, sess)
	}

	return sessions, nil
}
