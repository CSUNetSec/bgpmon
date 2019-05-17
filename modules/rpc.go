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

// rpcServer is the basic object which handles all RPC calls.
type rpcServer struct {
	*BaseDaemon

	grpcServer  *grpc.Server
	timeoutSecs int
}

// Run on the rpc server expects two options named "address" and "timeoutSecs"
func (r *rpcServer) Run(opts map[string]string) {
	defer r.wg.Done()

	if !util.CheckForKeys(opts, "address", "timeoutSecs") {
		r.logger.Errorf("options address and timeoutSecs not present")
		return
	}
	addr := opts["address"]
	tSecs := opts["timeoutSecs"]
	ts, err := strconv.ParseInt(tSecs, 10, 32)
	if err != nil {
		r.logger.Errorf("Error parsing timeoutSecs :%s", err)
		return
	}

	r.timeoutSecs = int(ts)
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		r.logger.Errorf("Error listening on address: %s", addr)
		return
	}

	r.grpcServer = grpc.NewServer()
	pb.RegisterBgpmondServer(r.grpcServer, r)

	err = r.grpcServer.Serve(listen)
	if err != nil {
		r.logger.Errorf("%s", err)
	}
}

func (r *rpcServer) Stop() error {
	if r.grpcServer == nil {
		return nil
	}

	r.grpcServer.GracefulStop()
	r.wg.Wait()
	return nil
}

// GetTimeout implements the util.GetTimeouter interface for RPC requests
func (r *rpcServer) GetTimeout() time.Duration {
	return time.Duration(r.timeoutSecs) * time.Second
}

// newRPCServer is the ModuleMaker for this module.
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
		start := time.Unix(int64(req.StartTimestamp), 0)
		end := time.Unix(int64(req.EndTimestamp), 0)
		fo := db.NewCaptureFilterOptions(req.CollectorName, start, end)
		stream, err = r.server.OpenReadStream(req.SessionId, db.SessionReadCapture, fo)
		if err != nil {
			return err
		}
	case pb.GetRequest_PREFIX:
		start := time.Unix(int64(req.StartTimestamp), 0)
		end := time.Unix(int64(req.EndTimestamp), 0)
		fo := db.NewCaptureFilterOptions(req.CollectorName, start, end)
		stream, err = r.server.OpenReadStream(req.SessionId, db.SessionReadPrefix, fo)
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
	if err == nil {
		r.logger.Infof("Session %s closed", request.SessionId)
	}

	return &pb.Empty{}, err
}

// ListOpenSession is the RPC port to the servers ListSessions function
func (r *rpcServer) ListOpenSessions(ctx context.Context, request *pb.Empty) (*pb.ListOpenSessionsReply, error) {
	var sessionIDs []string
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

	return nil, fmt.Errorf("session ID: %s not found", request.SessionId)
}

// Write is the RPC port to a server OpenWriteStream and writing to that stream.
func (r *rpcServer) Write(stream pb.Bgpmond_WriteServer) error {
	timeoutCtx, cf := context.WithTimeout(r.ctx, r.GetTimeout())
	defer cf()

	first, err := stream.Recv()
	if err != nil {
		return err
	}

	var writeType db.SessionType
	var objectFunc func(*pb.WriteRequest) (interface{}, error)

	switch first.Type {
	case pb.WriteRequest_BGP_CAPTURE:
		writeType = db.SessionWriteCapture
		objectFunc = func(wr *pb.WriteRequest) (interface{}, error) {
			cap, err := db.NewCaptureFromPB(wr.BgpCapture)
			return cap, err
		}
	case pb.WriteRequest_ENTITY:
		writeType = db.SessionWriteEntity
		objectFunc = func(wr *pb.WriteRequest) (interface{}, error) {
			ent, err := db.NewEntityFromPB(wr.Entity)
			return ent, err
		}
	default:
		return r.logger.Errorf("invalid write type")
	}

	err = r.WriteStream(timeoutCtx, stream, first, writeType, objectFunc)
	if err != nil {
		return err
	}
	rep := &pb.WriteReply{}
	err = stream.SendAndClose(rep)
	if err != nil {
		return err
	}

	r.logger.Infof("Write stream success")
	return nil
}

// WriteStream is a general purpose write method.
func (r *rpcServer) WriteStream(ctx context.Context,
	writeSrv pb.Bgpmond_WriteServer,
	firstMsg *pb.WriteRequest,
	writeType db.SessionType,
	getWriteObject func(*pb.WriteRequest) (interface{}, error)) error {

	stream, err := r.server.OpenWriteStream(firstMsg.SessionId, writeType)
	if err != nil {
		return err
	}
	defer stream.Close()

	obj, err := getWriteObject(firstMsg)
	if err == nil {
		err = stream.Write(obj)
		if err != nil {
			return err
		}
	} else {
		r.logger.Errorf("Error parsing object: %s", err)
	}

	for {
		if util.IsClosed(ctx) {
			stream.Cancel()
			return r.logger.Errorf("context closed")
		}

		wr, err := writeSrv.Recv()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				stream.Cancel()
				return err
			}
		}

		obj, err = getWriteObject(wr)
		if err != nil {
			r.logger.Errorf("Error parsing object: %s", err)
			continue
		}

		if err = stream.Write(obj); err != nil {
			stream.Cancel()
			return err
		}
	}

	if err = stream.Flush(); err != nil {
		return err
	}

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
