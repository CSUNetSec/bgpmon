package db

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/CSUNetSec/bgpmon/v2/config"
	"github.com/CSUNetSec/bgpmon/v2/util"
	_ "github.com/lib/pq"
	//pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/pkg/errors"
	"time"
)

const (
	CTXTIMEOUT = time.Duration(10) * time.Second
)

type sessionCmd int

const (
	SESSION_OPEN_STREAM sessionCmd = iota
)

type Sessioner interface {
	Do(cmd sessionCmd, arg interface{}) (interface{}, error)
	Close() error
}

type Session struct {
	uuid string
	ctx  context.Context
	wp   *util.WorkerPool
	dbo  *dbOper //this struct is responsible for providing the strings for the sql ops.
	db   *sql.DB
}

func (s *Session) Db() *sql.DB {
	return s.db
}

// Maybe this should return a channel that the calling function
// could read from to get the reply
func (s *Session) Do(cmd sessionCmd, arg interface{}) (interface{}, error) {
	switch cmd {
	case SESSION_OPEN_STREAM:

	}
	return nil, nil
}

func (s *Session) Close() error {
	return nil
}

func NewSession(ctx context.Context, conf config.SessionConfiger, id string, nworkers int) (Sessioner, error) {

	var (
		err    error
		constr string
		db     *sql.DB
	)
	wp := util.NewWorkerPool(nworkers)

	s := &Session{uuid: id, ctx: ctx, wp: wp}
	u := conf.GetUser()
	p := conf.GetPassword()
	d := conf.GetDatabaseName()
	h := conf.GetHostNames()
	cd := conf.GetCertDir()

	// The DB will need to be a field within session
	switch st := conf.GetTypeName(); st {
	case "postgres":
		s.dbo = newPostgressDboper()
		if len(h) == 1 && p != "" && cd == "" && u != "" { //no ssl standard pw
			constr = s.dbo.getdbop("connectNoSSL")
		} else if cd != "" && u != "" { //ssl
			constr = s.dbo.getdbop("connectSSL")
		} else {
			return nil, errors.New("Postgres sessions require a password and exactly one hostname")
		}
		db, err = sql.Open("postgres", fmt.Sprintf(constr, u, p, d, h[0]))
		if err != nil {
			return nil, errors.Wrap(err, "sql open")
		}

	case "cockroachdb":
		//sess, err = newCockroachSession(ctx, conf, id)
	default:
		return nil, errors.New("Unknown session type")
	}

	if err != nil {
		dblogger.Errorf("Failed openning session:%s", err)
	}
	s.db = db

	return s, err
}

//a wrapper struct that can contain all the possible arguments to our calls
type sqlIn struct {
	dbname     string                       //the name of the database we're operating on
	maintable  string                       //the table which references all collector-day tables.
	nodetable  string                       //the table with nodes and their configurations
	knownNodes map[string]config.NodeConfig //an incoming map of the known nodes
}

type sqlOut struct {
	ok         bool
	err        error
	knownNodes map[string]config.NodeConfig //a composition of the incoming and already known nodes
}

type workFunc func(sqlCtxExecutor, sqlIn) sqlOut

func (s *Session) doSyncNodes(known map[string]config.NodeConfig) (map[string]config.NodeConfig, error) {
	//ping to see we can connect to the db
	if err := s.db.Ping(); err != nil {
		return nil, errors.Wrap(err, "sql ping")
	}
	if pctx, err := GetNewExecutor(s.ctx, s, false, CTXTIMEOUT); err != nil {
		return nil, errors.Wrap(err, "newCtxTx")
	} else {
		snargs := sqlIn{dbname: "bgpmon", nodetable: "nodes", knownNodes: known}
		sex := newCtxTxSessionExecutor(pctx, s.dbo)
		syncNodes(sex, snargs)
	}
	return nil, nil
}

//Checks the state and initializes the appropriate schemas
func (s *Session) doCheckInit() error {
	//ping to see we can connect to the db
	if err := s.db.Ping(); err != nil {
		return errors.Wrap(err, "sql ping")
	}
	//check for required main tables on the schema.
	if pctx, err := GetNewExecutor(s.ctx, s, true, CTXTIMEOUT); err != nil {
		return errors.Wrap(err, "newCtxTx")
	} else {
		csargs := sqlIn{dbname: "bgpmon", maintable: "dbs", nodetable: "nodes"}
		sex := newCtxTxSessionExecutor(pctx, s.dbo)
		if ok, err := retCheckSchema(checkSchema(sex, csargs)); err != nil {
			return errors.Wrap(err, "retCheckschema")
		} else {
			if !ok { // table does not exist
				dblogger.Infof("creating schema tables")
				if err = retMakeSchema(makeSchema(sex, csargs)); err != nil {
					return errors.Wrap(err, "retMakeSchema")
				}
				dblogger.Infof("all good. commiting the changes")
				if err = pctx.Done(); err != nil {
					return errors.Wrap(err, "makeschema commit")
				}
			} else {
				dblogger.Infof("all main bgpmon tables exist.")
			}
		}
	}
	return nil
}
