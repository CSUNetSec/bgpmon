package db

import (
	"github.com/CSUNetSec/bgpmon/v2/config"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
)

func NewSession(conf config.SessionConfiger, id string) (Sessioner, error) {
	return &genericSession{}, nil
}

type genericSession struct {
}

func (gs *genericSession) Write(wr *pb.WriteRequest) error {
	dblogger.Infof("generic write called with request:%s", wr)
	return nil
}

func (gs *genericSession) Close() error {
	dblogger.Infof("generic close called")
	return nil
}

type Sessioner interface {
	Close() error
	Write(*pb.WriteRequest) error
}
