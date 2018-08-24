package db

import (
	"context"
	"database/sql"
	"github.com/CSUNetSec/bgpmon/v2/config"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/pkg/errors"
	"sort"
	"time"
)

const (
	CTXTIMEOUT = time.Duration(10) * time.Second
)

// sqlCtxExecutor is an interface needed for basic queries.
// It is implemented partly by both sql.DB and sql.Txn but we will
// implemented on our wrapper ctxTx. ctxTx wraps these functions with a context
// and deeper their xxxContext variant is called.
type sqlCtxExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

//creates a new ctxTx for that operation which implements the
//sqlExecutor interface. The argument passed instructs it to either
//do it on a transaction if true, or on the normal DB connection if false.
//caller must call Done() that releases resources.
func GetNewExecutor(pc context.Context, s Dber, doTx bool, ctxTimeout time.Duration) (*ctxTx, error) {
	var (
		tx  *sql.Tx
		err error
		db  *sql.DB
	)
	db = s.Db()
	ctx, cf := context.WithTimeout(pc, ctxTimeout)
	if doTx {
		if tx, err = db.BeginTx(pc, nil); err != nil {
			cf()
			return nil, err
		}
	} else {
		tx = nil
	}
	return &ctxTx{
		doTx: doTx,
		tx:   tx,
		cf:   cf,
		ctx:  ctx,
		db:   db,
	}, nil
}

func (c *ctxTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	if c.doTx && c.tx != nil {
		return c.tx.ExecContext(c.ctx, query, args...)
	}
	return c.db.ExecContext(c.ctx, query, args...)
}

func (c *ctxTx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if c.doTx && c.tx != nil {
		return c.tx.QueryContext(c.ctx, query, args...)
	}
	return c.db.QueryContext(c.ctx, query, args...)
}

func (c *ctxTx) QueryRow(query string, args ...interface{}) *sql.Row {
	if c.doTx && c.tx != nil {
		return c.tx.QueryRowContext(c.ctx, query, args...)
	}
	return c.db.QueryRowContext(c.ctx, query, args...)
}

//a wrapper of a sql.Tx that is able to accept multiple
//db ops and run them in the same tx.
//it will implement the sqlCtxExectutor interface and choose
//where to apply the sql function depending on how it was constructed.
//(either apply everything in the transaction and then the last Done()
//will commit, or straight on the DB and the last Done() is a noop.
//the ctxTx structs are created by the specific sessions.
type ctxTx struct {
	doTx bool
	tx   *sql.Tx
	db   *sql.DB
	cf   context.CancelFunc
	ctx  context.Context
}

//either commits the TX or just releases the context through it's cancelfunc.
func (ptx *ctxTx) Done() error {
	defer ptx.cf() //release resources if it's done.
	if ptx.doTx && ptx.tx != nil {
		return ptx.tx.Commit()
	}
	return nil
}

type sessionCmd int

const (
	SESSION_WRITE_MRT sessionCmd = iota
)

type Sessioner interface {
	Do(cmd sessionCmd, arg interface{}) (interface{}, error)
	Close() error
}

type Session struct {
	uuid     string
	workerCt int
	activeWk int
	lock     *sync.Mutex
}

// Maybe this should return a channel that the calling function
// could read from to get the reply
func (s *Session) Do(cmd sessionCmd, arg interface{}) (interface{}, error) {
	return nil, nil
}

func (s *Session) Close() error {
	return nil
}

type Dber interface {
	Db() *sql.DB
}

func NewSession(ctx context.Context, conf config.SessionConfiger, id string, nworkers int) (Sessioner, error) {

	s := &Session{uuid: id, workerCt: nworkers, activeWk: 0, lock: &sync.Mutex{}}

	// The DB will need to be a field within session
	switch st := conf.GetTypeName(); st {
	case "postgres":
		//sess, err = newPostgresSession(ctx, conf, id, nworkers)
	case "cockroachdb":
		//sess, err = newCockroachSession(ctx, conf, id)
	default:
		return nil, errors.New("Unknown session type")
	}

	if err != nil {
		dblogger.Errorf("Failed openning session:%s", err)
	}

	return s, err
}

//this struct is the element of an ordered array
//that will be used to name tables in the db and also
//refer to them when they are open.
type collectorDateString struct {
	colName   string
	startDate time.Time
	duration  time.Duration
}

func newCollectorDateString(name string, sd time.Time, dur time.Duration) *collectorDateString {
	return &collectorDateString{
		colName:   name,
		startDate: sd,
		duration:  dur,
	}
}

//collector-date strings ordered by their starting date.
type collectorsByNameDate []collectorDateString

//Len implementation for sort interface
func (c collectorsByNameDate) Len() int {
	return len(c)
}

//Swap implementation for sort interface
func (c collectorsByNameDate) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

//Less implementation for sort interface. uses the stable
//sort attribute to do two keys. first by name then by date.
func (c collectorsByNameDate) Less(i, j int) bool {
	if c[i].colName < c[j].colName {
		return true
	} else if c[i].colName > c[j].colName {
		return false
	}
	return c[i].startDate.Before(c[j].startDate)
}

//this will return the index and if the name and date are in the slice. caller has to check existence.
func (c collectorsByNameDate) colNameDateInSlice(colname string, date time.Time) (int, bool) {
	//find a possible index
	ind := sort.Search(c.Len(), func(i int) bool {
		return c[i].colName == colname && (c[i].startDate.After(date) || c[i].startDate.Equal(date))
	})
	//validate that the name is the same
	if c[ind].colName != colname {
		return 0, false
	}
	//catch exact same date
	if c[ind].startDate.Equal(date) {
		return ind, true
	}
	//catch the normal case where it is after the startdate, and before the startdate+duration
	if c[ind].startDate.Before(date) && date.Before(c[ind].startDate.Add(c[ind].duration)) {
		return ind, true
	}
	return 0, false
}

type genericSession struct {
	knownCollectors collectorsByNameDate
	parentCtx       context.Context
}

func (gs *genericSession) Write(wr *pb.WriteRequest) error {
	dblogger.Infof("generic write called with request:%s", wr)
	return nil
}

func (gs *genericSession) Close() error {
	dblogger.Infof("generic close called")
	return nil
}

func (ps *genericSession) Schema(SchemaCmd) SchemaReply {
	dblogger.Infof("generic SchemaCommand called")
	return SchemaReply{}
}

//implements dber
func (ps *genericSession) Db() *sql.DB {
	dblogger.Infof("generic Db called")
	return nil
}

func (ps *genericSession) GetParentContext() context.Context {
	dblogger.Infof("generic GetContext called")
	return ps.parentCtx
}
