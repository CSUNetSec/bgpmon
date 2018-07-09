package db

import (
	"database/sql"
	"fmt"
	"github.com/CSUNetSec/bgpmon/v2/config"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

type postgresSession struct {
	db *sql.DB
}

func (ps *postgresSession) Write(wr *pb.WriteRequest) error {
	dblogger.Infof("postgres write called with request:%s", wr)
	return nil
}

func (ps *postgresSession) Close() error {
	dblogger.Infof("postgres close called")
	if err := ps.db.Close(); err != nil {
		return errors.Wrap(err, "db close")
	}
	return nil
}

func newPostgresSession(conf config.SessionConfiger, id string) (Sessioner, error) {
	var (
		db  *sql.DB
		err error
	)
	dblogger.Infof("postgres db session starting")
	u := conf.GetUser()
	p := conf.GetPassword()
	d := conf.GetDatabaseName()
	h := conf.GetHostNames()
	if len(h) == 1 && p != "" {
		db, err = sql.Open("postgres", fmt.Sprintf("user=%s password=%s dbname=%s host=%s", u, p, d, h[0]))
		if err != nil {
			return nil, errors.Wrap(err, "sql open")
		}
	} else {
		return nil, errors.New("Postgres sessions require a password and exactly one hostname")
	}
	return &postgresSession{db: db}, nil
}
