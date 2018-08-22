package main

import (
	"fmt"
	"io"
	"net"
	"os"

	"github.com/CSUNetSec/bgpmon/v2/config"
	"github.com/CSUNetSec/bgpmon/v2/db"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/pkg/errors"

	"context"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net/http"
	_ "net/http/pprof"
)

var (
	mainlogger = logrus.WithField("system", "main")
)

type server struct {
	sessions   map[string]db.Sessioner // map from uuid to session interface
	conf       config.Configer         // the config populated from the file
	knownNodes map[string]config.NodeConfig
	ctx        context.Context
}

func newServer(c config.Configer) *server {
	return &server{
		knownNodes: c.GetConfiguredNodes(),
		sessions:   make(map[string]db.Sessioner),
		conf:       c,
		ctx:        context.Background(),
	}
}

//Get Messages from bgpmon
func (s *server) Get(req *pb.GetRequest, rep pb.Bgpmond_GetServer) error {
	switch req.Type {
	case pb.GetRequest_BGP_CAPTURE:
		mainlogger.Infof("Running Get with request:%v", req)
		return nil

	default:
		return errors.New("unimplemented Get request type")
	}
}

//Session RPC Calls
func (s *server) CloseSession(ctx context.Context, request *pb.CloseSessionRequest) (*pb.Empty, error) {
	mainlogger.Infof("Closing session %s", request.SessionId)
	sess, exists := s.sessions[request.SessionId]
	if !exists {
		return nil, errors.New(fmt.Sprintf("session ID %s not found", request.SessionId))
	} else {
		sess.Close()
		delete(s.sessions, request.SessionId)
	}

	mainlogger.Infof("Session %s closed", request.SessionId)
	return &pb.Empty{}, nil
}

func (s *server) ListOpenSessions(ctx context.Context, request *pb.Empty) (*pb.ListOpenSessionsReply, error) {
	sessionIDs := []string{}
	for sessionID, _ := range s.sessions {
		sessionIDs = append(sessionIDs, sessionID)
	}

	return &pb.ListOpenSessionsReply{SessionId: sessionIDs}, nil
}

func (s *server) ListAvailableSessions(ctx context.Context, request *pb.Empty) (*pb.ListAvailableSessionsReply, error) {
	availSessions := []*pb.SessionType{}
	for _, s := range s.conf.GetSessionConfigs() {
		availsess := &pb.SessionType{
			Name: s.GetName(),
			Type: s.GetTypeName(),
			Desc: fmt.Sprintf("hosts:%v", s.GetHostNames()),
		}

		availSessions = append(availSessions, availsess)
	}

	return &pb.ListAvailableSessionsReply{AvailableSessions: availSessions}, nil
}

func (s *server) OpenSession(ctx context.Context, request *pb.OpenSessionRequest) (*pb.OpenSessionReply, error) {
	mainlogger.Infof("Opening session named %s of config name:%s with %d workers", request.SessionId, request.SessionName, request.Workers)
	if _, exists := s.sessions[request.SessionId]; exists {
		return nil, errors.New(fmt.Sprintf("Session ID %s already exists", request.SessionId))
	}
	if sc, scerr := s.conf.GetSessionConfigWithName(request.SessionName); scerr != nil {
		return nil, scerr
	} else {
		//XXX here we are passing the background context of the server not the one in the argument.
		//therefore cancellation will be controlled by the server. consider using joincontext here.
		if sess, nserr := db.NewSession(s.ctx, sc, request.SessionId, int(request.Workers)); nserr != nil {
			return nil, errors.Wrap(nserr, "can't create session")
		} else {
			s.sessions[request.SessionId] = sess
			mainlogger.Infof("Session %s opened", request.SessionId)
			//check for configured nodes.
			sess.Schema(db.SchemaCmd{Cmd: db.SyncNodes})
		}
	}
	return &pb.OpenSessionReply{SessionId: request.SessionId}, nil
}

func (s *server) Write(stream pb.Bgpmond_WriteServer) error {
	var (
		sess   db.Sessioner
		first  bool
		exists bool
	)
	first = true
	for {
		writeRequest, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if first {
			if sess, exists = s.sessions[writeRequest.SessionId]; !exists {
				mainlogger.Errorf("session %s does not exist", writeRequest.SessionId)
				return errors.New(fmt.Sprintf("session %s does not exist", writeRequest.SessionId))
			}
			first = false
		}
		if err := sess.Write(writeRequest); err != nil {
			mainlogger.Errorf("error:%s writing on session:%s", err, writeRequest.SessionId)
			return errors.Wrap(err, "session write")
		}
	}
	mainlogger.Infof("write stream success")

	return nil
}

//getSessions looks for the sessions with IDs provided in the string slice, in the server's
//active session map, and returns those back.
func (s *server) getSessions(sessionIDs []string) ([]db.Sessioner, error) {
	sessions := []db.Sessioner{}
	for _, sessionID := range sessionIDs {
		sess, exists := s.sessions[sessionID]
		if !exists {
			return nil, errors.New(fmt.Sprintf("Session '%s' does not exist", sessionID))
		}

		sessions = append(sessions, sess)
	}
	return sessions, nil
}

func main() {
	if len(os.Args) != 2 {
		mainlogger.Fatal("no configuration file provided")
	}
	mainlogger.Infof("reading config file:%s", os.Args[1])
	if cfile, ferr := os.Open(os.Args[1]); ferr != nil {
		mainlogger.Fatalf("error opening configuration file:%s", ferr)
	} else if bc, cerr := config.NewConfig(cfile); cerr != nil {
		mainlogger.Fatalf("configuration error:%s", cerr)
	} else {
		cfile.Close()
		daemonConf := bc.GetDaemonConfig()
		if daemonConf.ProfilerOn {
			mainlogger.Infof("Starting pprof at address:%s", daemonConf.ProfilerHostPort)
			go func(addr string, log *logrus.Entry) {
				log.Fatal(http.ListenAndServe(addr, nil))
			}(daemonConf.ProfilerHostPort, mainlogger.WithField("system", "pprof"))
		}
		mainlogger.Infof("starting grpc server at address:%s", daemonConf.Address)
		if listen, lerr := net.Listen("tcp", daemonConf.Address); lerr != nil {
			mainlogger.Fatalf("setting up grpc server error:%s", lerr)
		} else {
			bgpmondServer := newServer(bc)
			grpcServer := grpc.NewServer()
			pb.RegisterBgpmondServer(grpcServer, bgpmondServer)
			grpcServer.Serve(listen)
		}
	}
}
