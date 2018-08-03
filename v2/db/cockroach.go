package db

import (
	"github.com/CSUNetSec/bgpmon/v2/config"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
)

type cockroachSession struct {
}

func (cs *cockroachSession) Write(wr *pb.WriteRequest) error {
	dblogger.Infof("cockroach write called with request:%s", wr)
	return nil
}

func (cs *cockroachSession) Close() error {
	dblogger.Infof("cockroach close called")
	return nil
}

func (cs *cockroachSession) Schema(SchemaCmd) SchemaReply {
	dblogger.Infof("cockroach SchemaCommand called")
	return SchemaReply{}
}

func newCockroachSession(conf config.SessionConfiger, id string) (Sessioner, error) {
	dblogger.Infof("cockroach db session starting")
	return &cockroachSession{}, nil
}
