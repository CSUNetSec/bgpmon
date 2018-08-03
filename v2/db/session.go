package db

import (
	"github.com/CSUNetSec/bgpmon/v2/config"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/pkg/errors"
	"sort"
	"time"
)

func NewSession(conf config.SessionConfiger, id string) (Sessioner, error) {
	var (
		sess Sessioner
		err  error
	)
	switch st := conf.GetTypeName(); st {
	case "postgres":
		sess, err = newPostgresSession(conf, id)
	case "cockroachdb":
		sess, err = newCockroachSession(conf, id)
	default:
		return nil, errors.New("Unknown session type")
	}
	if err != nil {
		dblogger.Errorf("Failed openning session:%s", err)
	} else {
		dblogger.Info("issuing schema check and init on the session")
		schemrep := sess.Schema(SchemaCmd{cmd: CheckAndInit})
		if schemrep.err != nil {
			return nil, errors.Wrap(schemrep.err, "NewSession schema checkAndInit")
		}
	}
	return sess, err
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
}

func (gs *genericSession) Write(wr *pb.WriteRequest) error {
	dblogger.Infof("generic write called with request:%s", wr)
	return nil
}

func (gs *genericSession) Close() error {
	dblogger.Infof("generic close called")
	return nil
}

func (ps genericSession) Schema(SchemaCmd) SchemaReply {
	dblogger.Infof("generic SchemaCommand called")
	return SchemaReply{}
}

const (
	CheckAndInit = iota
	WriteCollectors
	ReadCollectors
)

//schema commands have to be implemented by the databases
//and should be "atomic" in nature. the approach that seems
//correct is to have one goroutine doing all the schema work
//listening on channels for these commands to coordinate the
//others
type SchemaCmd struct {
	cmd int
}

type SchemaReply struct {
	err error
}

type Sessioner interface {
	Close() error
	Write(*pb.WriteRequest) error
	Schema(SchemaCmd) SchemaReply
}
