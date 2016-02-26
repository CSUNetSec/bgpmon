package main

import (
	"errors"
	"flag"
	"fmt"
	"net"

	"github.com/CSUNetSec/bgpmon/log"
	"github.com/CSUNetSec/bgpmon/module"
	"github.com/CSUNetSec/bgpmon/module/bgp"
	"github.com/CSUNetSec/bgpmon/module/gobgp"
	pb "github.com/CSUNetSec/bgpmon/protobuf"
	"github.com/CSUNetSec/bgpmon/session"

	"github.com/BurntSushi/toml"
	"github.com/google/uuid"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var configFile string
var bgpmondConfig BgpmondConfig

type BgpmondConfig struct {
	Address  string
	DebugOut string
	ErrorOut string
	Modules  ModuleConfig
	Sessions SessionConfig
}

type ModuleConfig struct {
	PrefixHijack bgp.PrefixHijackConfig
	GoBGPLink    gobgp.GoBGPLinkConfig
}

type SessionConfig struct {
	Cassandra session.CassandraConfig
	File      session.FileConfig
}

func init() {
	flag.StringVar(&configFile, "config_file", "", "bgpmond toml configuration file")
}

func main() {
	flag.Parse()

	if _, err := toml.DecodeFile(configFile, &bgpmondConfig); err != nil {
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
		sessions: make(map[string]session.Session),
		modules:  make(map[string]module.Moduler),
	}

	grpcServer := grpc.NewServer()
	pb.RegisterBgpmondServer(grpcServer, bgpmondServer)
	grpcServer.Serve(listen)
}

type Server struct {
	sessions map[string]session.Session //map from uuid to session interface
	modules  map[string]module.Moduler  //map from uuid to running module interface
}

/*
 * Module RPC Calls
 */
func (s Server) RunModule(ctx context.Context, config *pb.RunModuleConfig) (*pb.RunModuleResult, error) {
	var mod module.Moduler
	var err error

	switch config.Type {
	case pb.ModuleType_PREFIX_HIJACK:
		mod, err = s.createModule(config.GetPrefixHijackModule())
		if err != nil {
			break
		}
	default:
		return nil, errors.New("unimplemented module type")
	}

	if err != nil {
		return nil, err
	}

	mod.GetCommandChannel() <- module.ModuleCommand{module.COMRUN, nil}
	//TODO need to kill after execution ends
	return &pb.RunModuleResult{mod.Status()}, nil
}

func (s Server) StartModule(ctx context.Context, config *pb.StartModuleConfig) (*pb.StartModuleResult, error) {
	var mod module.Moduler
	var err error

	switch config.Type {
	case pb.ModuleType_GOBGP_LINK:
		mod, err = s.createModule(config.GetGobgpLinkModule())
		if err != nil {
			break
		}

		mod.GetCommandChannel() <- module.ModuleCommand{module.COMRUN, nil}
	case pb.ModuleType_PREFIX_HIJACK:
		rpcConfig := config.GetPrefixHijackModule()
		mod, err = s.createModule(rpcConfig)
		if err != nil {
			break
		}

		module.SchedulePeriodic(mod, rpcConfig.PeriodicSeconds, rpcConfig.TimeoutSeconds)
	default:
		return nil, errors.New("unimplemented module type")
	}

	if err != nil {
		return nil, err
	}

	moduleID := newID()
	s.modules[moduleID] = mod
	return &pb.StartModuleResult{moduleID}, nil
}

func (s Server) ListModules(ctx context.Context, config *pb.Empty) (*pb.ListModulesResult, error) {
	moduleIDs := []string{}
	for moduleID, _ := range s.modules {
		moduleIDs = append(moduleIDs, moduleID)
	}

	return &pb.ListModulesResult{moduleIDs}, nil
}

func (s Server) StopModule(ctx context.Context, config *pb.StopModuleConfig) (*pb.Empty, error) {
	mod, ok := s.modules[config.ModuleId]

	if !ok {
		return nil, errors.New("module ID not found")
	} else {
		command, _ := module.NewModuleCommand("COMDIE", nil)
		mod.GetCommandChannel() <- *command
		delete(s.modules, config.ModuleId)
	}

	return &pb.Empty{}, nil
}

/*
 * Session RPC Calls
 */
func (s Server) CloseSession(ctx context.Context, config *pb.CloseSessionConfig) (*pb.Empty, error) {
	sess, ok := s.sessions[config.SessionId]

	if !ok {
		return nil, errors.New("session ID not found")
	} else {
		sess.Close()
		delete(s.sessions, config.SessionId)
	}

	return &pb.Empty{}, nil
}

func (s Server) ListSessions(ctx context.Context, config *pb.Empty) (*pb.ListSessionsResult, error) {
	sessionIDs := []string{}
	for sessionID, _ := range s.sessions {
		sessionIDs = append(sessionIDs, sessionID)
	}

	return &pb.ListSessionsResult{sessionIDs}, nil
}

func (s Server) OpenSession(ctx context.Context, config *pb.OpenSessionConfig) (*pb.OpenSessionResult, error) {
	var sess session.Session
	var err error

	switch config.Type {
	case pb.SessionType_CASSANDRA:
		rpcConfig := config.GetCassandraSession()
		sess, err = session.NewCassandraSession(rpcConfig.Username, rpcConfig.Password, rpcConfig.Hosts, bgpmondConfig.Sessions.Cassandra)
		if err != nil {
			break
		}
	case pb.SessionType_FILE:
		rpcConfig := config.GetFileSession()
		sess, err = session.NewFileSession(rpcConfig.Filename, bgpmondConfig.Sessions.File)
		if err != nil {
			break
		}
	default:
		return nil, errors.New("unimplemented session type")
	}

	if err != nil {
		return nil, err
	}

	sessionID := newID()
	s.sessions[sessionID] = sess
	return &pb.OpenSessionResult{sessionID}, nil
}

/*
 * Miscellaneous Functions
 */

func newID() string {
	return uuid.New()
}

func (s Server) createModule(config interface{}) (module.Moduler, error) {
	var mod module.Moduler

	switch config.(type) {
	case *pb.GoBGPLinkModule:
		rpcConfig := config.(*pb.GoBGPLinkModule)
		outSessions, err := s.getSessions(rpcConfig.OutSessionId)
		if err != nil {
			return nil, err
		}

		mod, err = gobgp.NewGoBGPLinkModule(rpcConfig.Address, outSessions, bgpmondConfig.Modules.GoBGPLink)
		if err != nil {
			return nil, err
		}
	case *pb.PrefixHijackModule:
		rpcConfig := config.(*pb.PrefixHijackModule)
		inSessions, err := s.getSessions(rpcConfig.InSessionId)
		if err != nil {
			return nil, err
		}

		mod, err = bgp.NewPrefixHijackModule(rpcConfig.Prefix, rpcConfig.AsNumber, rpcConfig.PeriodicSeconds, rpcConfig.TimeoutSeconds, inSessions, bgpmondConfig.Modules.PrefixHijack)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unimplemented module type")
	}

	module.Init(mod)
	return mod, nil
}

func (s Server) getSessions(sessionIDs []string) ([]session.Session, error) {
	sessions := []session.Session{}
	for _, sessionID := range sessionIDs {
		sess, ok := s.sessions[sessionID]
		if !ok {
			return nil, errors.New(fmt.Sprintf("session with id '%s' does not exist", sessionID))
		}

		sessions = append(sessions, sess)
	}

	return sessions, nil
}
