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

// This is a very common error, so this is a simple function
// to generate it.
var (
	mkNxSessionErr = func(a string) error {
		return fmt.Errorf("Session ID: %s not found", a)
	}
)

// rpcServer is the basic object which handles all RPC calls.
type rpcServer struct {
	*BaseDaemon

	grpcServer  *grpc.Server
	timeoutSecs int
}

// Run on the rpc server expects two options named "address" and "timeoutsecs"
func (r *rpcServer) Run(opts map[string]string, finish core.FinishFunc) {
	defer r.wg.Done()
	// finish is not deferred because it should only be called when an error
	// occurs

	if !util.CheckForKeys(opts, "address", "timeoutsecs") {
		r.logger.Errorf("options address and timeoutsecs not present")
		finish()
		return
	}
	addr := opts["address"]
	tsecs := opts["timeoutsecs"]
	ts, err := strconv.ParseInt(tsecs, 10, 32)
	if err != nil {
		r.logger.Errorf("Error parsing timeoutsecs :%s", err)
		finish()
		return
	}

	r.timeoutSecs = int(ts)
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		r.logger.Errorf("Error listening on address: %s", addr)
		finish()
		return
	}

	r.grpcServer = grpc.NewServer()
	pb.RegisterBgpmondServer(r.grpcServer, r)
	r.grpcServer.Serve(listen)
	return
}

func (r *rpcServer) Stop() error {
	r.grpcServer.GracefulStop()
	r.wg.Wait()
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

	var stream db.ReadStream
	var err error

	switch req.Type {
	case pb.GetRequest_CAPTURE:
		rf := db.NewReadFilter(req.CollectorName, time.Unix(int64(req.StartTimestamp), 0), time.Unix(int64(req.EndTimestamp), 0))
		stream, err = r.server.OpenReadStream(req.SessionId, db.SessionReadCapture, rf)
		if err != nil {
			return err
		}
	case pb.GetRequest_PREFIX:
		rf := db.NewReadFilter(req.CollectorName, time.Unix(int64(req.StartTimestamp), 0), time.Unix(int64(req.EndTimestamp), 0))
		stream, err = r.server.OpenReadStream(req.SessionId, db.SessionReadPrefix, rf)
		if err != nil {
			return err
		}
	case pb.GetRequest_ASPATH:
		return fmt.Errorf("Not implemented")
	}
	defer stream.Close()

	for stream.Read() {
		data := stream.Bytes()
		getRep := &pb.GetReply{
			Type:       req.Type,
			Error:      "",
			Incomplete: false,
			Chunk:      [][]byte{data},
		}

		err := rep.Send(getRep)
		if err != nil {
			return err
		}

	}

	if stream.Err() != nil && stream.Err() != io.EOF {
		return stream.Err()
	}

	return nil
}

// CloseSession is the RPC port to the servers CloseSession function
func (r *rpcServer) CloseSession(ctx context.Context, request *pb.CloseSessionRequest) (*pb.Empty, error) {
	r.logger.Infof("Closing session %s", request.SessionId)

	err := r.server.CloseSession(request.SessionId)

	r.logger.Infof("Session %s closed", request.SessionId)
	return &pb.Empty{}, err
}

// ListOpenSession is the RPC port to the servers ListSessions function
func (r *rpcServer) ListOpenSessions(ctx context.Context, request *pb.Empty) (*pb.ListOpenSessionsReply, error) {
	sessionIDs := []string{}
	for _, sh := range r.server.ListSessions() {
		sessionIDs = append(sessionIDs, sh.Name)
	}

	return &pb.ListOpenSessionsReply{SessionId: sessionIDs}, nil
}

// ListAvailableSessions is the RPC port to the servers ListSessionTypes function
func (r *rpcServer) ListAvailableSessions(ctx context.Context, request *pb.Empty) (*pb.ListAvailableSessionsReply, error) {
	availSessions := r.server.ListSessionTypes()

	return &pb.ListAvailableSessionsReply{AvailableSessions: availSessions}, nil
}

// OpenSession is the RPC port to the servers OpenSession function
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

// Write is the RPC port to a server OpenWriteStream and writing to that stream.
func (r *rpcServer) Write(stream pb.Bgpmond_WriteServer) error {
	// TODO: Replace first with sync.Once()?
	var (
		first    bool
		dbStream db.WriteStream
	)
	timeoutCtx, cf := context.WithTimeout(r.ctx, r.GetTimeout())
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

// RunModule is the RPC port to the servers RunModule function
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

// CloseModule is the RPC port to the servers CloseModule function
func (r *rpcServer) CloseModule(ctx context.Context, request *pb.CloseModuleRequest) (*pb.Empty, error) {
	err := r.server.CloseModule(request.Id)
	return &pb.Empty{}, err
}

// ListAvailableModules is the RPC port to the servers ListModuleTypes function
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

// ListOpenModules is the RPC port to the servers ListRunningModules function
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
