package db

import (
	"fmt"
	"github.com/CSUNetSec/bgpmon/v2/config"
	"github.com/CSUNetSec/bgpmon/v2/util"
	"github.com/sirupsen/logrus"
)

var (
	slogger = logrus.WithField("system", "schema")
)

type schemaCmdOp int

const (
	INITSCHEMA = schemaCmdOp(iota)
	CHECKSCHEMA
	GETNODE
	SYNCNODES
	CHECKTABLE
)

type schemaCmd struct {
	op  schemaCmdOp
	sin sqlIn
}

type schemaReply struct {
	err  error
	sout sqlOut
	ok   bool
}

type schemaMgr struct {
	iChan chan schemaCmd
	oChan chan schemaReply
	cChan chan bool
	sex   SessionExecutor           //this executor should be a dbSessionExecutor, not at tx.
	cols  util.CollectorsByNameDate //collector name dates sorted by date and name for quick lookup.
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

			case GETNODE:
				slogger.Infof("getting node name")
				ret.sout = getNode(s.sex, icmd.sin)
			case CHECKTABLE:
				slogger.Infof("checking existance of collector tables in cache")
				gcd := icmd.sin.getColDate
				if i, ok := s.cols.ColNameDateInSlice(gcd.col, gcd.dat); ok {
					ret.sout = sqlOut{
						resultColDate: collectorDate{
							col: s.cols[i].GetNameDateStr(),
						},
					}
				}
				ret.sout = getTable(s.sex, icmd.sin)
				if ret.err == errNoTable {
					slogger.Infof("No existing table for that time range:%s.Creating it", icmd.sin)
				} else if ret.err != nil {
					slogger.Errorf("getTable error:%s", ret.err)
				}

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

func (s *schemaMgr) checkSchema(dbname, maintable, nodetable string) (bool, error) {
	sin := sqlIn{dbname: dbname, maintable: maintable, nodetable: nodetable}
	cmdin := schemaCmd{op: CHECKSCHEMA, sin: sin}
	s.iChan <- cmdin
	sreply := <-s.oChan
	return sreply.sout.ok, sreply.sout.err
}

func (s *schemaMgr) makeSchema(dbname, maintable, nodetable string) error {
	sin := sqlIn{dbname: dbname, maintable: maintable, nodetable: nodetable}
	cmdin := schemaCmd{op: INITSCHEMA, sin: sin}
	s.iChan <- cmdin
	sreply := <-s.oChan
	return sreply.sout.err
}

func (s *schemaMgr) syncNodes(dbname, nodetable string, knownNodes map[string]config.NodeConfig) (map[string]config.NodeConfig, error) {
	sin := sqlIn{dbname: dbname, nodetable: nodetable, knownNodes: knownNodes}
	cmdin := schemaCmd{op: SYNCNODES, sin: sin}
	s.iChan <- cmdin
	sreply := <-s.oChan
	return sreply.sout.knownNodes, sreply.sout.err
}

func (s *schemaMgr) getNode(dbname, nodetable string, nodeName string, nodeIP string) (*node, error) {
	sin := sqlIn{dbname: dbname, nodetable: nodetable, getNodeName: nodeName, getNodeIP: nodeIP}
	cmdin := schemaCmd{op: GETNODE, sin: sin}
	s.iChan <- cmdin
	sreply := <-s.oChan
	return sreply.sout.resultNode, sreply.sout.err
}
