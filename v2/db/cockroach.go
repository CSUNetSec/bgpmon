package db

import (
	"context"
	"database/sql"
	"github.com/CSUNetSec/bgpmon/v2/config"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
)

type cockroachSession struct {
	parentCtx context.Context
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

func (cs *cockroachSession) GetDb() *sql.DB {
	dblogger.Infof("cockroach GetDb called")
	return nil
}

func (cs *cockroachSession) GetParentContext() context.Context {
	dblogger.Infof("cockroach GetContext called")
	return cs.parentCtx
}

func newCockroachSession(ctx context.Context, conf config.SessionConfiger, id string) (Sessioner, error) {
	dblogger.Infof("cockroach db session starting")
	return &cockroachSession{parentCtx: ctx}, nil
}
