package db

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/CSUNetSec/bgpmon/v2/config"
	"github.com/CSUNetSec/bgpmon/v2/util"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

const (
	ctxTimeout = time.Duration(120) * time.Second //XXX there is a write timeout in bgpmond too! merge
	bufferSize = 40
)

type sessionCmd int

const (
	//SessionOpenStream is an argument for Do
	SessionOpenStream sessionCmd = iota
	//SessionStreamWriteMRT is an argument for Send
	SessionStreamWriteMRT
)

//SessionStream accepts CommonMessages and returns CommonReplies.
//internally it synchronizes with the schema manager and keeps open buffers
//for efficient writes
type SessionStream struct {
	req     chan CommonMessage
	resp    chan CommonReply
	cancel  chan bool
	wp      *util.WorkerPool
	schema  *schemaMgr
	closed  bool
	db      Dber
	oper    *dbOper
	ex      *ctxtxOperExecutor
	buffers map[string]util.SQLBuffer
}

//NewSessionStream returns a newly allocated SessionStream
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

	ss.req = make(chan CommonMessage)
	ss.resp = make(chan CommonReply)
	ss.buffers = make(map[string]util.SQLBuffer)
	ctxTx, _ := GetNewExecutor(context.Background(), ss.db, true, ctxTimeout)
	ss.ex = newCtxTxSessionExecutor(ctxTx, ss.oper)

	go ss.listen(daemonCancel)
	return ss
}

//Send performs a type of send on the sessionstream with an arbitrary argument
//WARNING, sending after a close will cause a panic, and may hang
func (ss *SessionStream) Send(cmd sessionCmd, arg interface{}) error {
	var (
		table string
		ok    bool
	)
	wr := arg.(*pb.WriteRequest)
	mtime, cip, err := util.GetTimeColIP(wr)
	if err != nil {
		dblogger.Errorf("failed to get Collector IP:%v", err)
		return err
	}
	table, err = ss.schema.getTable("bgpmon", "dbs", "nodes", cip.String(), mtime)
	if err != nil {
		return err
	}

	ss.req <- newCaptureMessage(table, wr)
	resp, ok := <-ss.resp

	if !ok {
		return fmt.Errorf("Response channel closed")
	}
	return resp.Error()
}

//Flush is called when a stream finishes successfully
//It flushes all remaining buffers
func (ss *SessionStream) Flush() error {
	for key := range ss.buffers {
		ss.buffers[key].Flush()
	}
	ss.ex.Done()
	return nil
}

//Cancel is used when there is an error on the client-side,
//called to rollback all executed queries
func (ss *SessionStream) Cancel() error {
	ss.ex.SetError(fmt.Errorf("Session stream cancelled"))
	return nil
}

//Close is only for a normal close operation. A cancellation
//can only be done by Closing the parent session while
//the stream is still running
//This should be called by the same goroutine as the one calling Send
func (ss *SessionStream) Close() error {
	dblogger.Infof("Closing session stream")
	close(ss.cancel)
	close(ss.req)

	ss.wp.Done()
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

	for {
		select {
		case normal, open := <-cancel:
			if !open {
				continue
			}

			ss.closed = true
			if normal {
				return
			}
		case val, ok := <-ss.req:
			// Between the last message and this one, the channel was closed unexpectedly. Return the error
			if ss.closed {
				ss.resp <- newReply(fmt.Errorf("Session stream channel closed"))
			}
			// The ss.req channel might see it's close before the cancel channel.
			// If that happens, this will add an empty sqlIn to the buffer
			if ok {
				ss.resp <- newReply(ss.addToBuffer(val))
			}
		}
	}
}

func (ss *SessionStream) addToBuffer(msg CommonMessage) error {
	cMsg := msg.(captureMessage)

	tName := cMsg.getTableName()
	if _, ok := ss.buffers[tName]; !ok {
		dblogger.Infof("Creating new buffer for table: %s", tName)
		stmt := fmt.Sprintf(ss.oper.getdbop(insertCaptureTableOp), tName)
		ss.buffers[tName] = util.NewInsertBuffer(ss.ex, stmt, bufferSize, 9, true)
	}
	buf := ss.buffers[tName]
	// This actually returns a WriteRequest, not a BGPCapture, but all the utility functions were built around
	// WriteRequests
	cap := cMsg.getCapture()

	ts, colIP, _ := util.GetTimeColIP(cap)
	peerIP, err := util.GetPeerIP(cap)
	if err != nil {
		dblogger.Infof("Unable to parse peer ip, ignoring message")
		return nil
	}

	asPath := util.GetAsPath(cap)
	nextHop, err := util.GetNextHop(cap)
	if err != nil {
		nextHop = net.IPv4(0, 0, 0, 0)
	}
	origin := 0
	if len(asPath) != 0 {
		origin = asPath[len(asPath)-1]
	} else {
		origin = 0
	}
	//here if it errors and the return is nil, PrefixToPQArray should leave it and the schema should insert the default
	advertized, _ := util.GetAdvertizedPrefixes(cap)
	withdrawn, _ := util.GetWithdrawnPrefixes(cap)
	protoMsg := util.GetProtoMsg(cap)

	advArr := util.PrefixesToPQArray(advertized)
	wdrArr := util.PrefixesToPQArray(withdrawn)

	return buf.Add(ts, colIP.String(), peerIP.String(), pq.Array(asPath), nextHop.String(), origin, advArr, wdrArr, protoMsg)
}

//Sessioner is an interface that wraps Do and Close
type Sessioner interface {
	Do(cmd sessionCmd, arg interface{}) (*SessionStream, error)
	Close() error
}

//Session represents a session to the underlying db. It holds references to the schema manager and workerpool.
type Session struct {
	uuid   string
	cancel chan bool
	wp     *util.WorkerPool
	dbo    *dbOper //this struct provides the strings for the sql ops.
	db     *sql.DB
	schema *schemaMgr
	maxWC  int
}

//NewSession returns a newly allocated Session
func NewSession(parentCtx context.Context, conf config.SessionConfiger, id string, nworkers int) (*Session, error) {
	var (
		err    error
		constr string
		db     *sql.DB
	)

	cancel := make(chan bool)

	var wc int
	// The configuration will default to 0 if not specified,
	// and the nworkers will be 0 if that flag isn't provided on
	// bgpmon. This creates a sane default system.
	// If neither was specified, default to 1
	if nworkers == 0 && conf.GetWorkerCt() == 0 {
		wc = 1
	} else if nworkers == 0 { // If the client didn't request a WC, default to the server one
		wc = conf.GetWorkerCt()
	} else { // The user specified a worker count, go with that
		wc = nworkers
	}

	wp := util.NewWorkerPool(wc)
	s := &Session{uuid: id, cancel: cancel, wp: wp, maxWC: wc}
	u := conf.GetUser()
	p := conf.GetPassword()
	d := conf.GetDatabaseName()
	h := conf.GetHostNames()
	cd := conf.GetCertDir()
	cn := conf.GetConfiguredNodes()

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
	sex := newDbSessionExecutor(s.db, s.dbo)

	s.schema = newSchemaMgr(sex)
	if err := s.schema.makeSchema(d, "dbs", "nodes"); err != nil {
		return nil, err
	}
	//calling syncnodes on the new schema manager
	nMsg := newNodesMessage(cn)
	nRep := syncNodes(sex, nMsg).(nodesReply)
	fmt.Print("merged nodes, from the config file and the db are:")
	config.PutConfiguredNodes(nRep.GetNodes(), os.Stdout)
	return s, nil
}

//Db satisfies the Dber interface on a Session
func (s *Session) Db() *sql.DB {
	return s.db
}

//Do returns a SessionStream that will provide the output from the Do command
//Maybe this should return a channel that the calling function
//could read from to get the reply
func (s *Session) Do(cmd sessionCmd, arg interface{}) (*SessionStream, error) {
	switch cmd {
	case SessionOpenStream:
		dblogger.Infof("Opening stream on session: %s", s.uuid)
		s.wp.Add()
		ss := NewSessionStream(s.cancel, s.wp, s.schema, s, s.dbo)
		return ss, nil
	}
	return nil, nil
}

//Close stops the schema manager and the worker pool
func (s *Session) Close() error {
	dblogger.Infof("Closing session: %s", s.uuid)

	close(s.cancel)
	s.wp.Close()
	s.schema.stop()

	return nil
}

//GetMaxWorkers returns the maximum amount of workers that the session supports
func (s *Session) GetMaxWorkers() int {
	return s.maxWC
}
