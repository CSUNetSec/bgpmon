package db

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/CSUNetSec/bgpmon/v2/config"
)

var (
	slogger = logrus.WithField("system", "schema")
)

type schemaCmdOp int

const (
	INITSCHEMA = schemaCmdOp(iota)
	CHECKSCHEMA
	GETCOLNAME
	SYNCNODES
	CHECKTABLE
)

type schemaCmd struct {
	op            schemaCmdOp
	sin	      sqlIn
	collectorIP   string
	tablename     string
}

type schemaReply struct {
	err       error
	sout      sqlOut
	ok	  bool
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
func (s *schemaMgr) run() {
	for {
		ret := schemaReply{ok: true}
		select {
		case icmd := <-s.iChan:
			switch icmd.op {
			case CHECKSCHEMA:
				slogger.Infof("checking correctness of db schema")
				ret.sout = checkSchema(s.sex, icmd.sin)

			case INITSCHEMA:
				slogger.Infof("initializing db schema")
				ret.sout = makeSchema(s.sex, icmd.sin)

			case SYNCNODES:
				slogger.Infof("syncing node configs")
				ret.sout = syncNodes(s.sex, icmd.sin)

			case GETCOLNAME:
				slogger.Infof("getting collector name for collector IP:%s", icmd.collectorIP)

			default:
				ret.err = fmt.Errorf("unhandled schema manager command:%+v", icmd)
				ret.ok = false
				slogger.Errorf("error::%s", ret.err)
			}
			s.oChan <- ret
		case <-s.cChan:
			slogger.Infof("schemaMgr terminating by close channel")
			return
		}
		s.oChan <- ret
	}
}

func (s *schemaMgr) stop() {
	close(s.cChan)
}

func (s *schemaMgr) checkSchema(dbname,maintable,nodetable string) (bool, error) {
	sin := sqlIn{dbname:dbname, maintable:maintable, nodetable:nodetable}
	cmdin := schemaCmd{op:CHECKSCHEMA, sin: sin}
	s.iChan <- cmdin
	sreply := <-s.oChan
	return sreply.sout.ok, sreply.sout.err
}

func (s *schemaMgr) makeSchema(dbname, maintable, nodetable string) error {
	sin := sqlIn{dbname:dbname, maintable:maintable, nodetable:nodetable}
	cmdin := schemaCmd{op:INITSCHEMA, sin: sin}
	s.iChan <- cmdin
	sreply := <-s.oChan
	return sreply.sout.err
}

func (s *schemaMgr) syncNodes(dbname, nodetable string, knownNodes map[string]config.NodeConfig) (map[string]config.NodeConfig, error) {
	sin := sqlIn{dbname:dbname, nodetable: nodetable, knownNodes: knownNodes}
	cmdin := schemaCmd{op:SYNCNODES, sin: sin}
	s.iChan <- cmdin
	sreply := <- s.oChan
	return sreply.sout.knownNodes, sreply.sout.err
}

