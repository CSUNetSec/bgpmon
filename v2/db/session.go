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
	SESSION_STREAM_WRITE_MRT
)

type SessionStream struct {
	req    chan sqlIn
	resp   chan sqlOut
	cancel chan bool
	wp     *util.WorkerPool
	ctx    context.Context
	cf     context.CancelFunc
}

func NewSessionStream(cancel chan bool, wp *util.WorkerPool) *SessionStream {
	ss := &SessionStream{}
	ss.wp = wp
	ss.cancel = cancel
	ss.req = make(chan sqlIn)
	ss.resp = make(chan sqlOut)

	go ss.listen()
	return ss
}

// WARNING, sending after a close will cause a panic, and may hang
func (ss *SessionStream) Send(cmd sessionCmd, arg interface{}) error {
	dblogger.Infof("Sending to session stream")
	//ss.req <- arg.(sqlIn)
	ss.req <- sqlIn{}
	resp, ok := <-ss.resp

	if !ok {
		return fmt.Errorf("Response channel closed")
	}
	return resp.err
}

func (ss *SessionStream) Flush() error {
	return nil
}

// This is the SessionStream goroutine
func (ss *SessionStream) listen() {
	defer dblogger.Infof("Session stream closed successfully")
	defer close(ss.resp)

	for {
		select {
		// Alive should be true if the stream was closed from ss.Close(), and false
		// if it was closed from a session close
		case _, alive := <-ss.cancel:
			_, ok := <-ss.req

			if ok && alive {
				dblogger.Errorf("cancel and request channel live at the same time, should be impossible")
				return
			}

			if ok {
				ss.resp <- sqlOut{ok: false, err: fmt.Errorf("Stream closed")}
			}
			// Otherwise, just let it die
			return
		case <-ss.req:
			ss.resp <- sqlOut{ok: true}
		}
	}
}

// This is only for a normal close operation. A cancellation
// can only be done by Close()'ing the parent session while
// the stream is still running
// This should be called by the same goroutine as the one calling
// send
func (ss *SessionStream) Close() error {
	dblogger.Infof("Closing session stream")

	close(ss.req)
	ss.cancel <- true

	ss.wp.Done()
	return nil
}

type Sessioner interface {
	Do(cmd sessionCmd, arg interface{}) (*SessionStream, error)
	Close() error
}

type Session struct {
	uuid   string
	cancel chan bool
	ctx    context.Context
	cf     context.CancelFunc
	wp     *util.WorkerPool
	dbo    *dbOper //this struct is responsible for providing the strings for the sql ops.
	db     *sql.DB
}

func (s *Session) Db() *sql.DB {
	return s.db
}

// Maybe this should return a channel that the calling function
// could read from to get the reply
func (s *Session) Do(cmd sessionCmd, arg interface{}) (*SessionStream, error) {
	switch cmd {
	case SESSION_OPEN_STREAM:
		dblogger.Infof("Opening stream on session: %s", s.uuid)
		s.wp.Add()
		ss := NewSessionStream(s.cancel, s.wp)
		return ss, nil
	}
	return nil, nil
}

func (s *Session) Close() error {
	close(s.cancel)
	s.wp.Close()
	dblogger.Infof("Closing session: %s", s.uuid)
	return nil
}

func NewSession(parentCtx context.Context, conf config.SessionConfiger, id string, nworkers int) (Sessioner, error) {

	var (
		err    error
		constr string
		db     *sql.DB
	)
	wp := util.NewWorkerPool(nworkers)

	ctx, cf := context.WithCancel(context.Background())
	cancel := make(chan bool)

	s := &Session{uuid: id, ctx: ctx, cf: cf, cancel: cancel, wp: wp}
	u := conf.GetUser()
	p := conf.GetPassword()
	d := conf.GetDatabaseName()
	h := conf.GetHostNames()
	cd := conf.GetCertDir()

	// The DB will need to be a field within session
	switch st := conf.GetTypeName(); st {
	case "postgres":
		s.dbo = newPostgressDbOper()
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
