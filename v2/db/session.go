package db

import (
	"github.com/CSUNetSec/bgpmon/v2/config"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/pkg/errors"
)

func NewSession(conf config.SessionConfiger, id string) (Sessioner, error) {
	var (
		sess Sessioner
		err  error
	)
	switch st := conf.GetTypeName(); st {
	case "postgres":
		sess, err = newPostgresSession(conf, id)
	case "cockroachdb":
		sess, err = newCockroachSession(conf, id)
	default:
		return nil, errors.New("Unknown session type")
	}
	return sess, err
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
