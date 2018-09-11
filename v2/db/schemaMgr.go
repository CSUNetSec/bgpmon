package db

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"time"
)

var (
	slogger = logrus.WithField("system", "schema")
)

type schemaCmdOp int

const (
	CHECKTABLE = schemaCmdOp(iota)
	INITSCHEMA
	CHECKSCHEMA
	MAKESCHEMA
	GETCOLNAME
)

type schemaCmd struct {
	op            schemaCmdOp
	tableName     string
	collectorName string
	collectorIP   string
	msgTimestamp  time.Time
}

type schemaReply struct {
	err       error
	ok        bool
	tableName string
}

type schemaMgr struct {
	iChan chan schemaCmd
	oChan chan schemaReply
	cChan chan bool
	sex   SessionExecutor //this executor should be a dbSessionExecutor, not at tx.
}

func newSchemaMgr(sex SessionExecutor) *schemaMgr {
	return &schemaMgr{
		iChan: make(chan schemaCmd),
		oChan: make(chan schemaReply),
		cChan: make(chan bool),
		sex:   sex,
	}
}

//should be run in a separate goroutine
func (s *schemaMgr) Run() {
	for {
		ret := schemaReply{ok: true}
		select {
		case icmd := <-s.iChan:
			switch icmd.op {
			case CHECKSCHEMA:
				slogger.Infof("checking correctness of db schema")

			case INITSCHEMA:
				slogger.Infof("initializing db schema")

			case CHECKTABLE:
				slogger.Infof("checking for table:%s", icmd.tableName)

			case GETCOLNAME:
				slogger.Infof("getting collector name for collector IP:%s", icmd.collectorIP)

			default:
				ret.err = fmt.Errorf("unhandled schema manager command:%+v", icmd)
				ret.ok = false
				slogger.Errorf("error::%s", ret.err)
			}
		case <-s.cChan:
			slogger.Infof("schemaMgr terminating by close channel")
			return
		}
		s.oChan <- ret
	}
}

func (s *schemaMgr) Close() {
	close(s.cChan)
}
