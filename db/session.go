package db

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/CSUNetSec/bgpmon/config"

	"github.com/pkg/errors"
	swg "github.com/remeh/sizedwaitgroup"
)

type sessionType int

const (
	// SessionWriteCapture is provided to a Sessioner's OpenWriteStream to open
	// a capture write stream
	SessionWriteCapture sessionType = iota
	//SessionReadCapture is provided to a Sessioner's OpenReadStream to open a
	// read capture stream
	SessionReadCapture
)

type sessionStream struct {
	db     TimeoutDber
	oper   *dbOper
	ex     *ctxtxOperExecutor
	schema *schemaMgr
	wp     *swg.SizedWaitGroup
}

func newSessionStream(db TimeoutDber, oper *dbOper, s *schemaMgr, wp *swg.SizedWaitGroup) *sessionStream {
	return &sessionStream{db: db, oper: oper, schema: s, wp: wp, ex: nil}
}

// ReadStream represents the different kinds of read streams that can be done on a session
type ReadStream interface {
}

// WriteStream represents the different kind of write streams that can be done on a session
type WriteStream interface {
	Write(interface{}) error
	Flush() error
	Cancel()
	Close()
}

//Sessioner is an interface that wraps the stream open functions and close
type Sessioner interface {
	OpenWriteStream(sessionType) (WriteStream, error)
	OpenReadStream(sessionType) (ReadStream, error)
	Close() error
}

//Session represents a session to the underlying db. It holds references to the schema manager and workerpool.
type Session struct {
	uuid          string
	cancel        chan bool
	wp            *swg.SizedWaitGroup
	dbo           *dbOper //this struct provides the strings for the sql ops.
	db            *sql.DB
	schema        *schemaMgr
	maxWC         int
	dbTimeoutSecs int
}

//NewSession returns a newly allocated Session
func NewSession(conf config.SessionConfiger, id string, nworkers int) (*Session, error) {
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
	} else if nworkers == 0 {
		// If the client didn't request a WC, default to the server one
		wc = conf.GetWorkerCt()
	} else {
		// The user specified a worker count, go with that
		wc = nworkers
	}

	wp := swg.New(wc)
	dt := conf.GetDBTimeoutSecs()
	s := &Session{uuid: id, cancel: cancel, wp: &wp, maxWC: wc, dbTimeoutSecs: dt}
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

//GetTimeout satisfies the GetTimeouter interface on a Session so it can be a TimeoutDber
func (s *Session) GetTimeout() time.Duration {
	return time.Duration(s.dbTimeoutSecs) * time.Second
}

// OpenWriteStream opens and returns a WriteStream with the given type, or an
// error if no such type exists
func (s *Session) OpenWriteStream(sType sessionType) (WriteStream, error) {
	switch sType {
	case SessionWriteCapture:
		s.wp.Add()
		parStream := newSessionStream(s, s.dbo, s.schema, s.wp)
		ws := newWriteCapStream(parStream, s.cancel)
		return ws, nil
	default:
		return nil, fmt.Errorf("unsupported write stream type")
	}
}

// OpenReadStream opens and returns a ReadStream with the given type, or an
// error if no such type exists
func (s *Session) OpenReadStream(sType sessionType) (ReadStream, error) {
	switch sType {
	case SessionReadCapture:
		return nil, fmt.Errorf("ReadCapture not yet supported")
	default:
		return nil, fmt.Errorf("unsupported read stream type")
	}
}

//Close stops the schema manager and the worker pool
func (s *Session) Close() error {
	dblogger.Infof("Closing session: %s", s.uuid)

	close(s.cancel)
	s.wp.Wait()
	s.schema.stop()

	return nil
}

//GetMaxWorkers returns the maximum amount of workers that the session supports
func (s *Session) GetMaxWorkers() int {
	return s.maxWC
}
