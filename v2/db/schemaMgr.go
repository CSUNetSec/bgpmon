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
	iChan     chan schemaCmd
	oChan     chan schemaReply
	sex       SessionExecutor           //this executor should be a dbSessionExecutor, not at tx.
	cols      util.CollectorsByNameDate //collector name dates sorted by date and name for quick lookup.
	nodeNames map[string]string
}

// This function launches the run method in a separate goroutine
func newSchemaMgr(sex SessionExecutor) *schemaMgr {
	sm := &schemaMgr{
		iChan:     make(chan schemaCmd),
		oChan:     make(chan schemaReply),
		sex:       sex,
		nodeNames: make(map[string]string),
	}
	go sm.run()
	return sm
}

//should be run in a separate goroutine
func (s *schemaMgr) run() {
	defer slogger.Infof("Schema manager closed successfully")
	defer close(s.oChan)

	var err error
	for {
		select {
		case icmd, ok := <-s.iChan:
			if !ok {
				return
			}
			ret := schemaReply{ok: true}

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
				capTableName, ok := s.checkTableCache(icmd.sin.getColDate.col, icmd.sin.getColDate.dat.UTC())
				if ok {
					ret.sout = sqlOut{capTable: capTableName}
				} else {
					slogger.Infof("Table cache miss. Creating table for col: %s date: %s", icmd.sin.getColDate.col, icmd.sin.getColDate.dat)
					ret.sout, err = s.makeCapTable(icmd.sin)
					if err != nil {
						slogger.Errorf("schemaMgr: %s", err)
						ret.ok = false
					}
				}
			default:
				ret.err = fmt.Errorf("unhandled schema manager command:%+v", icmd)
				ret.ok = false
				slogger.Errorf("error:%s", ret.err)
			}

			// Send the result back on the channel
			s.oChan <- ret
		}
	}
}

func (s *schemaMgr) makeCapTable(sin sqlIn) (sqlOut, error) {
	res := getTable(s.sex, sin)
	var (
		nodename, nodeip string
		stime, etime     time.Time
	)
	if res.err == errNoTable {
		nodeip = sin.getColDate.col
		sin.getNodeIP = sin.getColDate.col //we need to set this so that it appears it is coming from a getnode call
		nodesRes := getNode(s.sex, sin)
		//nodesRes, err := s.getNode(sin.dbname, sin.nodetable, "", sin.getColDate.col) //the colname is empty. we don't know it yet.
		if nodesRes.err != nil {
			return sqlOut{}, fmt.Errorf("makeCapTable: %s", nodesRes.err)
		}
		//we resolved the node, now calling getnodetablenamedates to get the fields for the new tablename
		tname, stime, etime := util.GetNodeTableNameDates(nodesRes.resultNode.nodeName, sin.getColDate.dat, nodesRes.resultNode.nodeDuration)
		nodename = nodesRes.resultNode.nodeName
		//making a new sqlin to create a new table
		nsin := sqlIn{maintable: sin.maintable, capTableCol: nodename}
		nsin.capTableName, nsin.capTableSdate, nsin.capTableEdate = tname, stime, etime
		nsout := createCaptureTable(s.sex, nsin)
		if nsout.err != nil {
			return sqlOut{}, fmt.Errorf("makeCapTable: %s", nsout.err)
		}
		res = nsout //will return the new sqlout at exit
	} else if res.err == nil {
		// we have a node table already and res contains the correct vaules to be added in the cache
		nodename, nodeip, stime, etime = res.resultNode.nodeName, res.resultNode.nodeIP, res.capStime, res.capEtime
	} else {
		return sqlOut{}, fmt.Errorf("makeCapTable: %s", res.err)
	}
	s.AddNodeAndTableInCache(nodename, nodeip, stime, etime)
	return res, nil
}

//this helper function sets the name-ip association in the nodemap of the schema mgr , and adds the
//collectordate in the schemamgr's collectorsbydate array correctly by resetting the reference
func (s *schemaMgr) AddNodeAndTableInCache(col string, colip string, sd time.Time, ed time.Time) {
	s.nodeNames[colip] = col
	newcols := s.cols.Add(col, sd, ed)
	s.cols = newcols
}

func (s *schemaMgr) checkTableCache(collectorip string, date time.Time) (string, bool) {
	if colname, nodeok := s.nodeNames[collectorip]; !nodeok {
		return "", false
	} else {
		i, ok := s.cols.ColNameDateInSlice(colname, date)
		if ok {
			return s.cols[i].GetNameDateStr(), true
		}
	}
	return "", false
}

// Below this are the interface methods, called by the session streams

// This doesn't need a dedicated close channel. With the way we use it,
// none of the other interface methods will be called after close is called.
func (s *schemaMgr) stop() {
	close(s.iChan)
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

func (s *schemaMgr) getTable(dbname, maintable, nodetable, ipstr string, date time.Time) (string, error) {
	coldate := collectorDate{
		dat: date,
		col: ipstr,
	}
	sin := sqlIn{dbname: dbname, maintable: maintable, nodetable: nodetable, getColDate: coldate}
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
