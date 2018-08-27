package db

import (
	"context"
	"database/sql"
	"github.com/CSUNetSec/bgpmon/v2/config"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/pkg/errors"
	"sort"
	"sync"
	"time"
)

const (
	CTXTIMEOUT = time.Duration(10) * time.Second
)

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
	maxWk    int
	activeWk int
	lock     *sync.Mutex
	ctx      context.Context
}

// Maybe this should return a channel that the calling function
// could read from to get the reply
func (s *Session) Do(cmd sessionCmd, arg interface{}) (interface{}, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.activeWk >= s.maxWk {
		// Receive from a 'done' channel
	}

	return nil, nil
}

func (s *Session) Close() error {
	return nil
}

func NewSession(ctx context.Context, conf config.SessionConfiger, id string, nworkers int) (Sessioner, error) {

	var err error
	s := &Session{uuid: id, maxWk: nworkers, activeWk: 0, lock: &sync.Mutex{}, ctx: ctx}

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

//implements dber
func (ps *genericSession) Db() *sql.DB {
	dblogger.Infof("generic Db called")
	return nil
}

func (ps *genericSession) GetParentContext() context.Context {
	dblogger.Infof("generic GetContext called")
	return ps.parentCtx
}
