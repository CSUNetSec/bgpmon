package db

import (
	"fmt"

	"github.com/CSUNetSec/bgpmon/config"
	"github.com/CSUNetSec/bgpmon/util"
	"time"
)

var (
	slogger = util.NewLogger("system", "schema")
)

const (
	mgrInitSchemaOp = iota
	mgrCheckSchemaOp
	mgrGetNodeOp
	mgrSyncNodesOp
	mgrGetTableOp
)

type schemaMgr struct {
	iChan     chan schemaMessage
	oChan     chan CommonReply
	sex       SessionExecutor           //this executor should be a dbSessionExecutor, not at tx.
	cols      util.CollectorsByNameDate //collector name dates sorted by date and name for quick lookup.
	nodeNames map[string]string
}

// This function launches the run method in a separate goroutine
func newSchemaMgr(sex SessionExecutor) *schemaMgr {
	sm := &schemaMgr{
		iChan:     make(chan schemaMessage),
		oChan:     make(chan CommonReply),
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

	for {
		select {
		case icmd, ok := <-s.iChan:
			if !ok {
				return
			}
			var ret CommonReply

			switch icmd.GetType() {
			case mgrCheckSchemaOp:
				slogger.Infof("checking correctness of db schema")
				ret = checkSchema(s.sex, icmd)
			case mgrInitSchemaOp:
				slogger.Infof("initializing db schema")
				ret = makeSchema(s.sex, icmd)
			case mgrSyncNodesOp:
				slogger.Infof("syncing node configs")
				ret = syncNodes(s.sex, icmd)
			case mgrGetNodeOp:
				slogger.Infof("getting node name")
				ret = getNode(s.sex, icmd)
			case mgrGetTableOp:
				tMsg := icmd.GetMessage().(tableMessage)
				cd := tMsg.getColDate()
				capTableName, sTime, eTime, ok := s.checkTableCache(cd.col, cd.dat.UTC())
				if ok {
					// Most of these fields have default values, by design
					ret = newTableReply(capTableName, sTime, eTime, nil, nil)
				} else {
					slogger.Infof("Table cache miss. Creating table for col: %s date: %s", cd.col, cd.dat)
					ret = s.makeCapTable(tMsg)
					if ret.Error() != nil {
						slogger.Errorf("schemaMgr: %s", ret.Error())
					}
				}
			default:
				ret = newReply(fmt.Errorf("unhandled schema manager command:%+v", icmd))
			}

			// Send the result back on the channel
			s.oChan <- ret
		}
	}
}

func (s *schemaMgr) makeCapTable(msg CommonMessage) CommonReply {
	res := getTable(s.sex, msg).(tableReply)
	var (
		nodename, nodeip string
		stime, etime     time.Time
	)

	if err := res.Error(); err == errNoTable {
		tMsg := msg.(tableMessage)
		cd := tMsg.getColDate()
		nodeip = cd.col
		// This name is intentionally left blank
		nodeRes := getNode(s.sex, newNodeMessage("", nodeip)).(nodeReply)
		//nodesRes, err := s.getNode(sin.dbname, sin.nodetable, "", sin.getColDate.col) //the colname is empty. we don't know it yet.
		if err := nodeRes.Error(); err != nil {
			return newReply(fmt.Errorf("makeCapTable: %s", err))
		}
		//we resolved the node, now calling getnodetablenamedates to get the fields for the new tablename
		tname, stime, etime := util.GetNodeTableNameDates(nodeRes.GetNode().nodeName, cd.dat, nodeRes.GetNode().nodeDuration)
		nodename = nodeRes.GetNode().nodeName

		cMsg := newCapTableMessage(tname, nodename, stime, etime)
		nsout := createCaptureTable(s.sex, cMsg).(capTableReply)
		if err := nsout.Error(); err != nil {
			return newReply(fmt.Errorf("makeCapTable: %s", err))
		}
		s, e := nsout.GetDates()
		res = newTableReply(nsout.GetName(), s, e, nodeRes.GetNode(), nil)
	} else if err == nil {
		// we have a node table already and res contains the correct vaules to be added in the cache
		stime, etime = res.getDates()
		nodename, nodeip = res.getNode().nodeName, res.getNode().nodeIP
	} else {
		return newReply(fmt.Errorf("makeCapTable: %s", err))
	}
	s.AddNodeAndTableInCache(nodename, nodeip, stime, etime)
	return res
}

//this helper function sets the name-ip association in the nodemap of the schema mgr , and adds the
//collectordate in the schemamgr's collectorsbydate array correctly by resetting the reference
func (s *schemaMgr) AddNodeAndTableInCache(col string, colip string, sd time.Time, ed time.Time) {
	s.nodeNames[colip] = col
	newcols := s.cols.Add(col, sd, ed)
	s.cols = newcols
}

func (s *schemaMgr) checkTableCache(collectorip string, date time.Time) (string, time.Time, time.Time, bool) {
	colname, nodeok := s.nodeNames[collectorip]
	if !nodeok {
		return "", time.Time{}, time.Time{}, false
	}
	i, ok := s.cols.ColNameDateInSlice(colname, date)
	if ok {
		_, nameDateStr, sTime, eTime := s.cols[i].GetNameDates()
		return nameDateStr, sTime, eTime, true
	}
	return "", time.Time{}, time.Time{}, false
}

// Below this are the interface methods, called by the session streams

// This doesn't need a dedicated close channel. With the way we use it,
// none of the other interface methods will be called after close is called.
func (s *schemaMgr) stop() {
	close(s.iChan)
}

func (s *schemaMgr) checkSchema(dbname, maintable, nodetable string) (bool, error) {
	cmdin := newSchemaMessage(newCustomMessage(maintable, nodetable), mgrCheckSchemaOp)
	s.iChan <- cmdin
	sreply := <-s.oChan
	return sreply.Error() == nil, sreply.Error()
}

func (s *schemaMgr) makeSchema(dbname, maintable, nodetable string) error {
	cmdin := newSchemaMessage(newCustomMessage(maintable, nodetable), mgrInitSchemaOp)
	s.iChan <- cmdin
	sreply := <-s.oChan
	return sreply.Error()
}

func (s *schemaMgr) syncNodes(dbname, nodetable string, knownNodes map[string]config.NodeConfig) (map[string]config.NodeConfig, error) {
	nMsg := newNodesMessage(knownNodes)
	nMsg.SetNodeTable(nodetable)
	cmdin := newSchemaMessage(nMsg, mgrSyncNodesOp)
	s.iChan <- cmdin
	sreply := <-s.oChan
	nRep := sreply.(nodesReply)
	return nRep.GetNodes(), nRep.Error()
}

func (s *schemaMgr) getTable(dbname, maintable, nodetable, ipstr string, date time.Time) (string, time.Time, time.Time, error) {
	coldate := collectorDate{
		dat: date,
		col: ipstr,
	}
	tMsg := newTableMessage(coldate)
	tMsg.SetMainTable(maintable)
	tMsg.SetNodeTable(nodetable)
	cmdin := newSchemaMessage(tMsg, mgrGetTableOp)
	s.iChan <- cmdin
	sreply := <-s.oChan

	if sreply.Error() != nil {
		return "", time.Time{}, time.Time{}, sreply.Error()
	}
	tRep := sreply.(tableReply)
	tName := tRep.getName()
	tStart, tEnd := tRep.getDates()
	return tName, tStart, tEnd, tRep.Error()
}

func (s *schemaMgr) getNode(dbname, nodetable string, nodeName string, nodeIP string) (*node, error) {
	nMsg := newNodeMessage(nodeName, nodeIP)
	nMsg.SetNodeTable(nodetable)
	cmdin := newSchemaMessage(nMsg, mgrGetNodeOp)
	s.iChan <- cmdin
	sreply := <-s.oChan
	nRep := sreply.(nodeReply)
	return nRep.GetNode(), nRep.Error()
}
