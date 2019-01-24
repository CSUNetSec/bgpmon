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
	msg CommonMessage
}

type schemaReply struct {
	err error
	rep CommonReply
	ok  bool
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
				ret.rep = checkSchema(s.sex, icmd.msg)
			case INITSCHEMA:
				slogger.Infof("initializing db schema")
				ret.rep = makeSchema(s.sex, icmd.msg)
			case SYNCNODES:
				slogger.Infof("syncing node configs")
				ret.rep = syncNodes(s.sex, icmd.msg)
			case GETNODE:
				slogger.Infof("getting node name")
				ret.rep = getNode(s.sex, icmd.msg)
			case GETTABLE:
				tMsg := icmd.msg.(tableMessage)
				cd := tMsg.GetColDate()
				capTableName, ok := s.checkTableCache(cd.col, cd.dat.UTC())
				if ok {
					// Most of these fields have default values, by design
					ret.rep = NewTableReply(capTableName, time.Now(), time.Now(), nil, nil)
				} else {
					slogger.Infof("Table cache miss. Creating table for col: %s date: %s", cd.col, cd.dat)
					ret.rep, err = s.makeCapTable(tMsg)
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

func (s *schemaMgr) makeCapTable(msg CommonMessage) (CommonReply, error) {
	res := getTable(s.sex, msg).(tableReply)
	var (
		nodename, nodeip string
		stime, etime     time.Time
	)

	if res.GetErr() == errNoTable {
		tMsg := msg.(tableMessage)
		cd := tMsg.GetColDate()
		nodeip = cd.col
		// This name is intentionally left blank
		nodeRes := getNode(s.sex, NewNodeMessage("", nodeip)).(nodeReply)
		//nodesRes, err := s.getNode(sin.dbname, sin.nodetable, "", sin.getColDate.col) //the colname is empty. we don't know it yet.
		if nodeRes.GetErr() != nil {
			return nodeRes, fmt.Errorf("makeCapTable: %s", nodeRes.GetErr())
		}
		//we resolved the node, now calling getnodetablenamedates to get the fields for the new tablename
		tname, stime, etime := util.GetNodeTableNameDates(nodeRes.GetNode().nodeName, cd.dat, nodeRes.GetNode().nodeDuration)
		nodename = nodeRes.GetNode().nodeName

		cMsg := NewCapTableMessage(tname, nodename, stime, etime)
		nsout := createCaptureTable(s.sex, cMsg).(capTableReply)
		if nsout.GetErr() != nil {
			return nsout, fmt.Errorf("makeCapTable: %s", nsout.GetErr())
		}
		s, e := nsout.GetDates()
		res = NewTableReply(nsout.GetName(), s, e, nodeRes.GetNode(), nil)
	} else if res.GetErr() == nil {
		// we have a node table already and res contains the correct vaules to be added in the cache
		stime, etime = res.GetDates()
		nodename, nodeip = res.GetNode().nodeName, res.GetNode().nodeIP
	} else {
		return NewReply(nil), fmt.Errorf("makeCapTable: %s", res.GetErr())
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
	cmdin := schemaCmd{op: CHECKSCHEMA, msg: NewCustomMessage(maintable, nodetable)}
	s.iChan <- cmdin
	sreply := <-s.oChan
	return sreply.rep.GetErr() == nil, sreply.rep.GetErr()
}

func (s *schemaMgr) makeSchema(dbname, maintable, nodetable string) error {
	cmdin := schemaCmd{op: INITSCHEMA, msg: NewCustomMessage(maintable, nodetable)}
	s.iChan <- cmdin
	sreply := <-s.oChan
	return sreply.rep.GetErr()
}

func (s *schemaMgr) syncNodes(dbname, nodetable string, knownNodes map[string]config.NodeConfig) (map[string]config.NodeConfig, error) {
	nMsg := NewNodesMessage(knownNodes)
	nMsg.SetNodeTable(nodetable)
	cmdin := schemaCmd{op: SYNCNODES, msg: nMsg}
	s.iChan <- cmdin
	sreply := <-s.oChan
	nRep := sreply.rep.(nodesReply)
	return nRep.GetNodes(), nRep.GetErr()
}

func (s *schemaMgr) getTable(dbname, maintable, nodetable, ipstr string, date time.Time) (string, error) {
	coldate := collectorDate{
		dat: date,
		col: ipstr,
	}
	tMsg := NewTableMessage(coldate)
	tMsg.SetMainTable(maintable)
	tMsg.SetNodeTable(nodetable)
	cmdin := schemaCmd{op: GETTABLE, msg: tMsg}
	s.iChan <- cmdin
	sreply := <-s.oChan

	if !sreply.ok {
		return "", sreply.rep.GetErr()
	}
	tRep := sreply.rep.(tableReply)
	return tRep.GetName(), tRep.GetErr()
}

func (s *schemaMgr) getNode(dbname, nodetable string, nodeName string, nodeIP string) (*node, error) {
	nMsg := NewNodeMessage(nodeName, nodeIP)
	nMsg.SetNodeTable(nodetable)
	cmdin := schemaCmd{op: GETNODE, msg: nMsg}
	s.iChan <- cmdin
	sreply := <-s.oChan
	nRep := sreply.rep.(nodeReply)
	return nRep.GetNode(), nRep.GetErr()
}
