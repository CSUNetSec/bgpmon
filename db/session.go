package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/CSUNetSec/bgpmon/config"

	"github.com/pkg/errors"
	swg "github.com/remeh/sizedwaitgroup"
)

// SessionType describes the supported session streams.
type SessionType int

const (
	// SessionWriteCapture is provided to a Sessioner's OpenWriteStream to open
	// a capture write stream.
	SessionWriteCapture SessionType = iota
	// SessionReadCapture is provided to a Sessioner's OpenReadStream to open a
	// read capture stream.
	SessionReadCapture
	// SessionReadPrefix is provided to a Sessioner's OpenReadStream to open a
	// read prefix stream.
	SessionReadPrefix
)

type sessionStream struct {
	db     TimeoutDber
	oper   queryProvider
	schema *schemaMgr
	wp     *swg.SizedWaitGroup
}

func newSessionStream(db TimeoutDber, oper queryProvider, s *schemaMgr, wp *swg.SizedWaitGroup) *sessionStream {
	return &sessionStream{db: db, oper: oper, schema: s, wp: wp}
}

// ReadStream represents the different kinds of read streams that can be done on a session
type ReadStream interface {
	Read() bool

	// Data returns an interface so each implementation of a read stream is free to choose
	// what type it returns. This should be constant for each stream, and should be documented.
	Data() interface{}
	Bytes() []byte
	Err() error
	Close()
}

// WriteStream represents the different kind of write streams that can be done on a session
type WriteStream interface {
	Write(interface{}) error
	Flush() error
	Cancel()
	Close()
}

// Session represents a session to the underlying db. It holds references to the schema manager and workerpool.
type Session struct {
	uuid          string
	cancel        chan bool
	wp            *swg.SizedWaitGroup
	dbo           queryProvider //this struct provides the strings for the sql ops.
	db            *sql.DB
	schema        *schemaMgr
	maxWC         int
	dbTimeoutSecs int
}

// NewSession returns a newly allocated Session
func NewSession(conf config.SessionConfiger, id string, workers int) (*Session, error) {
	var (
		err    error
		constr string
		db     *sql.DB
	)

	cancel := make(chan bool)

	var wc int
	// The configuration will default to 0 if not specified,
	// and the workers will be 0 if that flag isn't provided on
	// bgpmon. This creates a sane default system.
	// If neither was specified, default to 1
	if workers == 0 && conf.GetWorkerCt() == 0 {
		wc = 1
	} else if workers == 0 {
		// If the client didn't request a WC, default to the server one
		wc = conf.GetWorkerCt()
	} else {
		// The user specified a worker count, go with that
		wc = workers
	}

	wp := swg.New(wc)
	dt := conf.GetDBTimeoutSecs()
	s := &Session{uuid: id, cancel: cancel, wp: &wp, maxWC: wc, dbTimeoutSecs: dt}
	username := conf.GetUser()
	password := conf.GetPassword()
	dbName := conf.GetDatabaseName()
	hostNames := conf.GetHostNames()
	certDir := conf.GetCertDir()
	cn := conf.GetConfiguredNodes()

	// The DB will need to be a field within session
	switch st := conf.GetTypeName(); st {
	case "postgres":
		if len(hostNames) != 1 {
			return nil, fmt.Errorf("postgres sessions require exactly one hostname")
		}

		s.dbo = newPostgressQueryProvider()
		// If the user provided a username and password, but no cert dir,
		// don't use SSL
		if password != "" && certDir == "" && username != "" {
			constr = s.dbo.getQuery(connectNoSSLOp)
		} else if certDir != "" && username != "" {
			// Otherwise, use SSL
			constr = s.dbo.getQuery(connectSSLOp)
		} else {
			return nil, errors.New("postgres sessions require a password and exactly one hostname")
		}

		db, err = sql.Open("postgres", fmt.Sprintf(constr, username, password, dbName, hostNames[0]))
		if err != nil {
			return nil, errors.Wrap(err, "sql open")
		}
	case "cockroachdb":
		return nil, errors.New("cockroachdb not yet supported")
	default:
		return nil, errors.New("unknown session type")
	}
	s.db = db
	sEx := newSessionExecutor(s.db, s.dbo)
	s.schema = newSchemaMgr(sEx, defaultMainTable, defaultNodeTable, defaultEntityTable)

	if err := s.initDB(cn); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Session) initDB(cn map[string]config.NodeConfig) error {
	if err := s.schema.makeSchema(); err != nil {
		return err
	}

	nodes, err := s.schema.syncNodes(cn)
	if err != nil {
		dbLogger.Errorf("Error syncing nodes: %s", err)
	} else {
		dbLogger.Infof("Synced nodes, creating suggested nodes file")
		err = config.PutConfiguredNodes(nodes)
		if err != nil {
			dbLogger.Errorf("Error writing configured nodes file: %s", err)
		}
	}
	return nil
}

// Db satisfies the Dber interface on a Session
func (s *Session) Db() *sql.DB {
	return s.db
}

// GetTimeout satisfies the GetTimeouter interface on a Session so it can be a TimeoutDber
func (s *Session) GetTimeout() time.Duration {
	return time.Duration(s.dbTimeoutSecs) * time.Second
}

// OpenWriteStream opens and returns a WriteStream with the given type, or an
// error if no such type exists
func (s *Session) OpenWriteStream(sType SessionType) (WriteStream, error) {
	switch sType {
	case SessionWriteCapture:
		s.wp.Add()
		parStream := newSessionStream(s, s.dbo, s.schema, s.wp)
		ws, err := newWriteCapStream(parStream, s.cancel)
		if err != nil {
			s.wp.Done()
		}
		return ws, err
	default:
		return nil, fmt.Errorf("unsupported write stream type")
	}
}

// OpenReadStream opens and returns a ReadStream with the given type, or an
// error if no such type exists
func (s *Session) OpenReadStream(sType SessionType, rf ReadFilter) (ReadStream, error) {
	switch sType {
	case SessionReadCapture:
		s.wp.Add()
		parStream := newSessionStream(s, s.dbo, s.schema, s.wp)
		rs := newReadCapStream(parStream, s.cancel, rf)
		return rs, nil
	case SessionReadPrefix:
		s.wp.Add()
		parStream := newSessionStream(s, s.dbo, s.schema, s.wp)
		rs := newReadPrefixStream(parStream, s.cancel, rf)
		return rs, nil
	default:
		return nil, fmt.Errorf("unsupported read stream type")
	}
}

// Close stops the schema manager and the worker pool
func (s *Session) Close() error {
	dbLogger.Infof("Closing session: %s", s.uuid)

	close(s.cancel)
	s.wp.Wait()
	s.schema.stop()

	return nil
}

// GetMaxWorkers returns the maximum amount of workers that the session supports
func (s *Session) GetMaxWorkers() int {
	return s.maxWC
}
