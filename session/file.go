package session

import (
	"fmt"
	"github.com/CSUNetSec/bgpmon/util"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon"
)

type FileConfig struct {
}

type FileSession struct {
	*Session
}

func NewFileSession(filename string, config FileConfig) (Sessioner, error) {
	writers := make(map[pb.WriteRequest_Type][]Writer)

	session, err := NewSession(writers, 1)
	if err != nil {
		return nil, err
	}

	return FileSession{&session}, nil
}

func (f FileSession) Close() error {
	return nil
}

func (f FileSession) Write(w *pb.WriteRequest) error {
	fmt.Printf("got request type:%+v\n", w.Type)
	switch w.Type {
	case pb.WriteRequest_BGP_STATS:
		fmt.Printf("%+v\n", util.AsStr2dot(util.DedupAspath(w.BgpStats)))
	default:
		fmt.Printf("unhandled request type:%+v\n", w.Type)
	}
	return nil
}
