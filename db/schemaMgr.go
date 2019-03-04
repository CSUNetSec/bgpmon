package db

import (
	"fmt"
	"net"

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
	iChan chan schemaMessage
	oChan chan CommonReply
	sex   SessionExecutor //this executor should be a dbSessionExecutor, not at tx.
	cache *dbCache
}

// This function launches the run method in a separate goroutine
func newSchemaMgr(sex SessionExecutor) *schemaMgr {
	sm := &schemaMgr{
		iChan: make(chan schemaMessage),
		oChan: make(chan CommonReply),
		sex:   sex,
		cache: newDBCache(),
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
				ret = checkSchema(s.sex, icmd.GetMessage())
			case mgrInitSchemaOp:
				slogger.Infof("initializing db schema")
				ret = makeSchema(s.sex, icmd.GetMessage())
			case mgrSyncNodesOp:
				slogger.Infof("syncing node configs")
				ret = syncNodes(s.sex, icmd.GetMessage())
			case mgrGetNodeOp:
				slogger.Infof("getting node name")
				nMsg := icmd.GetMessage().(nodeMessage)
				nodeIP := net.ParseIP(nMsg.getNodeIP())
				node, err := s.cache.LookupNode(nodeIP)
				if err == errNoNode {
					slogger.Infof("Node cache miss. Looking up node for IP: %s", nMsg.getNodeIP())
					ret = getNode(s.sex, icmd.GetMessage())
					if ret.Error() == nil {
						s.cache.addNode(ret.(nodeReply).GetNode())
					} else {
						slogger.Errorf("Error getting node: %s", ret.Error())
					}
				} else {
					ret = newNodeReply(node, nil)
				}
			case mgrGetTableOp:
				tMsg := icmd.GetMessage().(tableMessage)
				colIP := tMsg.GetColIP()
				date := tMsg.GetDate()
				tName, err := s.cache.LookupTable(net.ParseIP(colIP), date)
				if err == errNoTable {
					slogger.Infof("Table cache miss. Creating table for col: %s date: %s", colIP, date)
					ret = s.makeCapTable(tMsg)
					if ret.Error() != nil {
						slogger.Errorf("schemaMgr: %s", ret.Error())
					} else {
						s.cache.addTable(net.ParseIP(colIP), date)
					}
				} else {
					ret = newTableReply(tName, time.Now(), time.Now(), nil, nil)
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
		node *node
	)

	if err := res.Error(); err == errNoTable {
		tMsg := msg.(tableMessage)
		nodeip := tMsg.GetColIP()
		date := tMsg.GetDate()

		node, err := s.cache.LookupNode(net.ParseIP(nodeip))
		if err != nil {
			return newReply(err)
		}
		ddm := time.Duration(node.nodeDuration) * time.Minute
		start := date.Truncate(ddm)

		tName := genTableName(node.nodeName, date, node.nodeDuration)
		cMsg := newCapTableMessage(tName, node.nodeName, start, start.Add(ddm))
		nsout := createCaptureTable(s.sex, cMsg).(capTableReply)
		if err := nsout.Error(); err != nil {
			return newReply(fmt.Errorf("makeCapTable: %s", err))
		}
		s, e := nsout.GetDates()
		res = newTableReply(nsout.GetName(), s, e, node, nil)
	} else if err == nil {
		// we have a node table already and res contains the correct vaules to be added in the cache
		node = res.getNode()
		s.cache.addNode(node)
	} else {
		return newReply(fmt.Errorf("makeCapTable: %s", err))
	}
	return res
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
	tMsg := newTableMessage(ipstr, date)
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

func (s *schemaMgr) LookupTable(nodeIP net.IP, t time.Time) (string, error) {
	tName, _, _, err := s.getTable("bgpmon", "dbs", "nodes", nodeIP.String(), t)
	return tName, err
}

func (s *schemaMgr) LookupNode(nodeIP net.IP) (*node, error) {
	n, err := s.getNode("bgpmon", "nodes", "", nodeIP.String())
	return n, err
}
