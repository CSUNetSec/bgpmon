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
	"net"
	"time"
)

const (
	CTXTIMEOUT = time.Duration(120) * time.Second //XXX there is a write timeout in bgpmond too! merge
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
	db     Dber
	oper   *dbOper
	ex     *ctxtxOperExecutor
}

func NewSessionStream(pcancel chan bool, wp *util.WorkerPool, smgr *schemaMgr, db Dber, oper *dbOper) *SessionStream {
	ss := &SessionStream{closed: false, wp: wp, schema: smgr, db: db, oper: oper}

	parentCancel := pcancel
	childCancel := make(chan bool)
	daemonCancel := make(chan bool)
	// If the parent requests a close, and the routine isn't already closed,
	// pass that on to the child.
	go func(par, child, daemon chan bool) {
		select {
		case <-par:
			daemon <- false
		case <-child:
			daemon <- true
		}
		close(daemon)
	}(parentCancel, childCancel, daemonCancel)
	ss.cancel = childCancel

	ss.req = make(chan sqlIn)
	ss.resp = make(chan sqlOut)
	ctxTx, _ := GetNewExecutor(context.Background(), ss.db, true, CTXTIMEOUT)
	ss.ex = newCtxTxSessionExecutor(ctxTx, ss.oper)

	go ss.listen(daemonCancel)
	return ss
}

// WARNING, sending after a close will cause a panic, and may hang
func (ss *SessionStream) Send(cmd sessionCmd, arg interface{}) error {
	wr := arg.(*pb.WriteRequest)
	mtime, cip, err := util.GetTimeColIP(wr)
	if err != nil {
		dblogger.Errorf("failed to get Collector IP:%v", err)
		return err
	}
	dblogger.Infof("Query schema for time: %v col:%v", mtime, cip)
	table, err := ss.schema.getTable("bgpmon", "dbs", cip.String(), mtime)
	if err != nil {
		return err
	}

	dblogger.Infof("table to write bgp capture:%v", table)
	ss.req <- sqlIn{capTableName: table, capture: wr}
	resp, ok := <-ss.resp

	dblogger.Infof("Finished writing message")
	if !ok {
		return fmt.Errorf("Response channel closed")
	}
	return resp.err
}

func (ss *SessionStream) Flush() error {
	ss.ex.tx.Commit()
	return nil
}

// This is the SessionStream goroutine
// This function is a little bit tricky, because a stream needs to be closable
// from two different directions.
// 1. A normal close. This is when a client calls Close on the SessionStream
//	  after it is done communicating with it.
//		We can assume that nothing more will come in on the request channel.
// 2. A session close. This occurs on an unexpected shutdown, such as ctrl-C.
//		A client may try to send requests to this after it has been closed. It
//		should return that the stream has been closed before shutting down
//		completely.
func (ss *SessionStream) listen(cancel chan bool) {
	defer dblogger.Infof("Session stream closed successfully")
	defer close(ss.resp)
	var err error

	for {
		select {
		case normal := <-cancel:
			ss.closed = true

			if !normal {
				ss.resp <- sqlOut{err: fmt.Errorf("Channel closed")}
			}
			return
		case val := <-ss.req:
			err = nil // reset the possible error
			args := captureSqlIn{capTableName: val.capTableName}
			args.id = util.GetUpdateID()
			args.timestamp, args.colIP, err = util.GetTimeColIP(val.capture)
			args.peerIP, err = util.GetPeerIP(val.capture)
			if err != nil {
				dblogger.Errorf("failed to get peer ip:%v. ignoring message", err)
				continue
			}
			args.asPath = util.GetAsPath(val.capture)
			args.nextHop, err = util.GetNextHop(val.capture)
			if err != nil { //non fatal error just log. XXX: the db must become resilent to null entries!
				dblogger.Errorf("could not get next hop:%v. setting it to null", err)
				args.nextHop = net.IPv4(0, 0, 0, 0)
			}
			args.origin = util.GetOriginAs(val.capture)
			args.isWithdraw = false
			args.protoMsg = []byte(val.capture.GetBgpCapture().String())

			ret := insertCapture(ss.ex, args)
			ss.resp <- ret
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
	close(ss.cancel)
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
	dbo    *dbOper //this struct provides the strings for the sql ops.
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
		ss := NewSessionStream(s.cancel, s.wp, s.schema, s, s.dbo)
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
