package modules

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	core "github.com/CSUNetSec/bgpmon"
	"github.com/CSUNetSec/bgpmon/db"
	"github.com/CSUNetSec/bgpmon/util"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"google.golang.org/grpc"
)

var (
	mkNxSessionErr = func(a string) error {
		return fmt.Errorf("Session ID: %s not found", a)
	}
)

type rpcServer struct {
	*BaseDaemon
	grpcServer  *grpc.Server
	timeoutSecs int
}

// Run on the rpc server expects two options named "address" and "timeoutsecs"
func (r *rpcServer) Run(opts map[string]string, finish core.FinishFunc) error {
	defer finish()

	if !util.CheckForKeys(opts, "address", "timeoutsecs") {
		return r.logger.Errorf("options address and timeoutsecs not present")
	}
	addr := opts["address"]
	tsecs := opts["timeoutsecs"]
	ts, err := strconv.ParseInt(tsecs, 10, 32)
	if err != nil {
		return r.logger.Errorf("Error parsing timeoutsecs :%s", err)
	}

	r.timeoutSecs = int(ts)
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		return r.logger.Errorf("Error listening on address: %s", addr)
	}

	r.grpcServer = grpc.NewServer()
	pb.RegisterBgpmondServer(r.grpcServer, r)
	r.grpcServer.Serve(listen)
	return nil
}

func (r *rpcServer) Stop() error {
	r.grpcServer.GracefulStop()
	return nil
}

// GetTimeout implements the util.GetTimeouter interface for RPC requests
func (r *rpcServer) GetTimeout() time.Duration {
	return time.Duration(r.timeoutSecs) * time.Second
}

func newRPCServer(s core.BgpmondServer, l util.Logger) core.Module {
	return &rpcServer{BaseDaemon: NewBaseDaemon(s, l, "rpc"), grpcServer: nil}
}

func init() {
	opts := "address : the address to start the RPC server\n" +
		"timeoutsecs : timeout for some RPC requests"
	rpcHandle := core.ModuleHandler{
		Info: core.ModuleInfo{
			Type:        "rpc",
			Description: "launch an RPC interface to the BgpmondServer",
			Opts:        opts,
		},
		Maker: newRPCServer,
	}
	core.RegisterModule(rpcHandle)
}

// Below are all the methods required for the RPC server
func (r *rpcServer) Get(req *pb.GetRequest, rep pb.Bgpmond_GetServer) error {
	r.logger.Infof("Running Get with request:%v", req)
	/*
		sh, exists := s.sessions[req.SessionId]
		if !exists {
			return mkNxSessionErr(req.SessionId)
		}

		switch req.Type {
		case pb.GetRequest_CAPTURE:
			cchan := db.GetCaptures(sh.sess, req)
			for capt := range cchan {
				if serr := rep.Send(&capt); serr != nil {
					return errors.Wrap(serr, "failed to send capture to client")
				}
			}
		case pb.GetRequest_ASPATH:
		case pb.GetRequest_PREFIX:

		default:
			return errors.New("unimplemented Get request type")
		}
	*/
	return nil
}

func (r *rpcServer) CloseSession(ctx context.Context, request *pb.CloseSessionRequest) (*pb.Empty, error) {
	r.logger.Infof("Closing session %s", request.SessionId)

	err := r.server.CloseSession(request.SessionId)

	r.logger.Infof("Session %s closed", request.SessionId)
	return &pb.Empty{}, err
}

func (r *rpcServer) ListOpenSessions(ctx context.Context, request *pb.Empty) (*pb.ListOpenSessionsReply, error) {
	sessionIDs := []string{}
	for _, sh := range r.server.ListSessions() {
		sessionIDs = append(sessionIDs, sh.Name)
	}

	return &pb.ListOpenSessionsReply{SessionId: sessionIDs}, nil
}

func (r *rpcServer) ListAvailableSessions(ctx context.Context, request *pb.Empty) (*pb.ListAvailableSessionsReply, error) {
	availSessions := r.server.ListSessionTypes()

	return &pb.ListAvailableSessionsReply{AvailableSessions: availSessions}, nil
}

func (r *rpcServer) OpenSession(ctx context.Context, request *pb.OpenSessionRequest) (*pb.OpenSessionReply, error) {
	r.logger.Infof("Opening session named %s of config name:%s with %d workers", request.SessionId, request.SessionName, request.Workers)

	err := r.server.OpenSession(request.SessionName, request.SessionId, int(request.Workers))
	return &pb.OpenSessionReply{SessionId: request.SessionId}, err
}

func (r *rpcServer) GetSessionInfo(ctx context.Context, request *pb.SessionInfoRequest) (*pb.SessionInfoReply, error) {
	r.logger.Infof("Returning info on session: %s", request.SessionId)
	for _, sh := range r.server.ListSessions() {
		if sh.Name == request.SessionId {
			return &pb.SessionInfoReply{Type: sh.SessType, SessionId: sh.Name, Workers: uint32(sh.Session.GetMaxWorkers())}, nil
		}
	}

	return nil, mkNxSessionErr(request.SessionId)
}

func (r *rpcServer) Write(stream pb.Bgpmond_WriteServer) error {
	var (
		first    bool
		dbStream db.WriteStream
	)
	timeoutCtx, cf := context.WithTimeout(context.Background(), r.GetTimeout())
	defer cf()

	first = true
	for {
		if util.IsClosed(timeoutCtx) {
			if dbStream != nil {
				dbStream.Cancel()
			}
			return r.logger.Errorf("context closed, aborting write")
		}

		writeRequest, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			if dbStream != nil {
				dbStream.Cancel()
			}
			return err
		}

		if first {
			dbStream, err = r.server.OpenWriteStream(writeRequest.SessionId)
			if err != nil {
				return r.logger.Errorf("error opening stream: %s", err)
			}
			first = false
			defer dbStream.Close()
		}

		if err := dbStream.Write(writeRequest); err != nil {
			dbStream.Cancel()
			return r.logger.Errorf("error writing on session(%s): %s", writeRequest.SessionId, err)
		}
	}
	if dbStream == nil {
		return r.logger.Errorf("session stream never created")
	}

	if err := dbStream.Flush(); err != nil {
		return r.logger.Errorf("write stream failed to flush: %s", err)
	}

	r.logger.Infof("Write stream success")
	return nil
}

func (r *rpcServer) RunModule(ctx context.Context, request *pb.RunModuleRequest) (*pb.RunModuleReply, error) {
	opts, err := util.StringToOptMap(request.Args)
	if err != nil {
		return nil, err
	}

	err = r.server.RunModule(request.Type, request.Id, opts)
	if err != nil {
		return nil, err
	}

	return &pb.RunModuleReply{Id: request.Id}, nil
}

func (r *rpcServer) CloseModule(ctx context.Context, request *pb.CloseModuleRequest) (*pb.Empty, error) {
	err := r.server.CloseModule(request.Id)
	return &pb.Empty{}, err
}

func (r *rpcServer) ListAvailableModules(ctx context.Context, _ *pb.Empty) (*pb.ListAvailableModulesReply, error) {
	var ret []*pb.ModuleInfo

	modules := r.server.ListModuleTypes()
	for _, v := range modules {
		info := &pb.ModuleInfo{
			Type: v.Type,
			Desc: v.Description,
			Opts: v.Opts,
		}
		ret = append(ret, info)
	}

	return &pb.ListAvailableModulesReply{AvailableModules: ret}, nil
}

func (r *rpcServer) ListOpenModules(ctx context.Context, _ *pb.Empty) (*pb.ListOpenModulesReply, error) {
	var ret []*pb.OpenModuleInfo

	modules := r.server.ListRunningModules()
	for _, v := range modules {
		info := &pb.OpenModuleInfo{
			Type:   v.Type,
			Id:     v.ID,
			Status: v.Status,
		}
		ret = append(ret, info)
	}

	return &pb.ListOpenModulesReply{OpenModules: ret}, nil
}
