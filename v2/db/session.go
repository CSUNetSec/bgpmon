package db

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/CSUNetSec/bgpmon/v2/config"
	"github.com/CSUNetSec/bgpmon/v2/util"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	_ "github.com/lib/pq"
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
	schema *schemaMgr
	closed bool
}

func NewSessionStream(cancel chan bool, wp *util.WorkerPool, smgr *schemaMgr) *SessionStream {
	ss := &SessionStream{closed: false}
	ss.wp = wp
	ss.cancel = cancel
	ss.schema = smgr
	ss.req = make(chan sqlIn)
	ss.resp = make(chan sqlOut)

	go ss.listen()
	return ss
}

// WARNING, sending after a close will cause a panic, and may hang
func (ss *SessionStream) Send(cmd sessionCmd, arg interface{}) error {
	wr := arg.(*pb.WriteRequest)
	dblogger.Infof("i got cmd:%+v type:%T arg:%+v", cmd, wr, wr)
	cip, mtime := util.GetTimeColIP(wr)
	dblogger.Infof("query schema for time %v col:%v", mtime, cip)
	table, err := ss.schema.getTable("bgpmon", "dbs", cip.String(), mtime)
	if err != nil {
		return err
	}
	dblogger.Infof("table to write bgp capture:%v", table)
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
			ss.closed = true
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
		case val := <-ss.req:
			dblogger.Infof("got %+v", val)
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
	if !ss.closed {
		ss.cancel <- true
	}

	close(ss.req)

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
	wp     *util.WorkerPool
	dbo    *dbOper //this struct is responsible for providing the strings for the sql ops.
	db     *sql.DB
	schema *schemaMgr
}

func NewSession(parentCtx context.Context, conf config.SessionConfiger, id string, nworkers int) (Sessioner, error) {
	var (
		err    error
		constr string
		db     *sql.DB
	)
	wp := util.NewWorkerPool(nworkers)

	cancel := make(chan bool)

	s := &Session{uuid: id, cancel: cancel, wp: wp}
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
		return nil, errors.New("cockroach not yet supported")
	default:
		return nil, errors.New("Unknown session type")
	}
	s.db = db

	s.schema = newSchemaMgr(newDbSessionExecutor(s.db, s.dbo))
	go s.schema.run()

	return s, nil
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
		ss := NewSessionStream(s.cancel, s.wp, s.schema)
		return ss, nil
	}
	return nil, nil
}

func (s *Session) Close() error {
	dblogger.Infof("Closing session: %s", s.uuid)

	close(s.cancel)
	s.wp.Close()
	s.schema.stop()

	return nil
}
