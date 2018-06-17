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

	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	mainlogger = logrus.WithField("system", "main")
)

type server struct {
	sessions map[string]db.Sessioner // map from uuid to session interface
	conf     config.Configer         // the config populated from the file
}

func newServer(c config.Configer) *server {
	return &server{
		sessions: make(map[string]db.Sessioner),
		conf:     c,
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
	mainlogger.Infof("Closing session %s\n", request.SessionId)
	sess, exists := s.sessions[request.SessionId]
	if !exists {
		return nil, errors.New(fmt.Sprintf("session ID %s not found", request.SessionId))
	} else {
		sess.Close()
		delete(s.sessions, request.SessionId)
	}

	mainlogger.Infof("Session %s closed\n", request.SessionId)
	return &pb.Empty{}, nil
}

func (s *server) ListOpenSessions(ctx context.Context, request *pb.Empty) (*pb.ListOpenSessionsReply, error) {
	sessionIDs := []string{}
	for sessionID, _ := range s.sessions {
		sessionIDs = append(sessionIDs, sessionID)
	}

	return &pb.ListOpenSessionsReply{sessionIDs}, nil
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

	return &pb.ListAvailableSessionsReply{availSessions}, nil
}

func (s *server) OpenSession(ctx context.Context, request *pb.OpenSessionRequest) (*pb.OpenSessionReply, error) {
	mainlogger.Infof("Opening session named %s of config name:%s with %d workers\n", request.SessionId, request.SessionName, request.Workers)
	if _, exists := s.sessions[request.SessionId]; exists {
		return nil, errors.New(fmt.Sprintf("Session ID %s already exists", request.SessionId))
	}
	if sc, scerr := s.conf.GetSessionConfigWithName(request.SessionName); scerr != nil {
		return nil, scerr
	} else {
		if sess, nserr := db.NewSession(sc, request.SessionId); nserr != nil {
			return nil, errors.Wrap(nserr, "can't create session")
		} else {
			s.sessions[request.SessionId] = sess
			mainlogger.Infof("Session %s opened\n", request.SessionId)
		}
	}
	return &pb.OpenSessionReply{request.SessionId}, nil
}

func (s *server) Write(stream pb.Bgpmond_WriteServer) error {
	for {
		writeRequest, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if sess, exists := s.sessions[writeRequest.SessionId]; !exists {
			mainlogger.Errorf("session %s does not exist", writeRequest.SessionId)
			return errors.New(fmt.Sprintf("session %s does not exist", writeRequest.SessionId))
		} else {
			if err := sess.Write(writeRequest); err != nil {
				mainlogger.Errorf("error:%s writing on session:%s\n", err, writeRequest.SessionId)
				return errors.Wrap(err, "session write")
			}
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
	mainlogger.Infof("reading config file:%s", os.Args[1])
	if bc, cerr := config.NewConfig(os.Args[1]); cerr != nil {
		mainlogger.Fatalf("configuration error:%s", cerr)
	} else {
		mainlogger.Infof("starting grpc server at address:%s", bc.GetListenAddress())
		if listen, lerr := net.Listen("tcp", bc.GetListenAddress()); lerr != nil {
			mainlogger.Fatalf("setting up grpc server error:%s", lerr)
		} else {
			bgpmondServer := newServer(bc)
			grpcServer := grpc.NewServer()
			pb.RegisterBgpmondServer(grpcServer, bgpmondServer)
			grpcServer.Serve(listen)
		}
	}
}
