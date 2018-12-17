package db

import (
	"fmt"
	"github.com/CSUNetSec/bgpmon/v2/config"
	"github.com/CSUNetSec/bgpmon/v2/util"
	"github.com/sirupsen/logrus"
	"time"
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
	GETTABLE
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

			case GETTABLE:
				slogger.Infof("checking existance of collector tables in cache")
				gcd := icmd.sin.getColDate
				if i, ok := s.cols.ColNameDateInSlice(gcd.col, gcd.dat); ok {
					ret.sout = sqlOut{
						resultColDate: collectorDate{
							col: s.cols[i].GetNameDateStr(),
						},
					}
				} else {
					ret.sout = getTable(s.sex, icmd.sin)
					if ret.sout.err == errNoTable {
						icmd.sin.nodetable = "nodes" //XXX hardcoded adding the table
						icmd.sin.getNodeIP = icmd.sin.getColDate.col
						slogger.Infof("No existing table for that time range:%+v. time is :%+v.Creating it", icmd.sin, icmd.sin.getColDate.dat)
						nodesout := getNode(s.sex, icmd.sin)
						slogger.Infof("name of that collector node:%v", nodesout.resultNode.nodeName)
						trunctime := icmd.sin.getColDate.dat.Truncate(time.Duration(nodesout.resultNode.nodeDuration) * time.Minute).UTC()
						slogger.Infof("truncated time is:%v", trunctime)
						nsin := sqlIn{maintable: icmd.sin.maintable}
						nsin.capTableCol = icmd.sin.getColDate.col
						nsin.capTableSdate = trunctime
						nsin.capTableEdate = trunctime.Add(time.Duration(nodesout.resultNode.nodeDuration) * time.Minute).UTC()
						nsin.capTableName = fmt.Sprintf("captures_%s_%s", nodesout.resultNode.nodeName, trunctime.Format("2006_01_02_15_04_05"))
						nsout := createCaptureTable(s.sex, nsin)
						if nsout.err != nil {
							slogger.Errorf("createCaptureTable error:%s", nsout.err)
						} else {
							ret.sout = nsout
						}
					} else if ret.sout.err != nil {
						slogger.Errorf("getTable error:%s", ret.err)
					}
				}

			default:
				ret.err = fmt.Errorf("unhandled schema manager command:%+v", icmd)
				ret.ok = false
				slogger.Errorf("error:%s", ret.err)
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

func (s *schemaMgr) getTable(dbname, maintable, ipstr string, date time.Time) (string, error) {
	coldate := collectorDate{
		dat: date,
		col: ipstr,
	}
	sin := sqlIn{dbname: dbname, maintable: maintable, getColDate: coldate}
	cmdin := schemaCmd{op: GETTABLE, sin: sin}
	s.iChan <- cmdin
	sreply := <-s.oChan
	return sreply.sout.capTable, sreply.sout.err
}

func (s *schemaMgr) getNode(dbname, nodetable string, nodeName string, nodeIP string) (*node, error) {
	sin := sqlIn{dbname: dbname, nodetable: nodetable, getNodeName: nodeName, getNodeIP: nodeIP}
	cmdin := schemaCmd{op: GETNODE, sin: sin}
	s.iChan <- cmdin
	sreply := <-s.oChan
	return sreply.sout.resultNode, sreply.sout.err
}