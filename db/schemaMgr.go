package db

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/CSUNetSec/bgpmon/config"
	"github.com/CSUNetSec/bgpmon/util"
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
	iChan    chan schemaMessage
	oChan    chan CommonReply
	sex      SessionExecutor //this executor should be a dbSessionExecutor, not at tx.
	cache    *dbCache
	daemonWG sync.WaitGroup

	mainTable   string
	nodeTable   string
	entityTable string
}

func (s *schemaMgr) getCommonMessage() CommonMessage {
	return newCustomMessage(s.mainTable, s.nodeTable, s.entityTable)
}

func (s *schemaMgr) setMessageTables(cm CommonMessage) {
	cm.SetMainTable(s.mainTable)
	cm.SetNodeTable(s.nodeTable)
	cm.SetEntityTable(s.entityTable)
}

// This function launches the run method in a separate goroutine
func newSchemaMgr(sex SessionExecutor, main, node, entity string) *schemaMgr {
	sm := &schemaMgr{
		iChan:       make(chan schemaMessage),
		oChan:       make(chan CommonReply),
		sex:         sex,
		cache:       newDBCache(),
		daemonWG:    sync.WaitGroup{},
		mainTable:   main,
		nodeTable:   node,
		entityTable: entity,
	}
	sm.daemonWG.Add(1)
	go sm.run()
	return sm
}

// Should be run in a separate goroutine
func (s *schemaMgr) run() {
	defer slogger.Infof("Schema manager closed successfully")
	defer close(s.oChan)
	defer s.daemonWG.Done()

	for {
		select {
		case icmd, ok := <-s.iChan:
			if !ok {
				return
			}
			var ret CommonReply

			switch icmd.getType() {
			case mgrCheckSchemaOp:
				slogger.Infof("checking correctness of db schema")
				ret = checkSchema(s.sex, icmd.getMessage())
			case mgrInitSchemaOp:
				slogger.Infof("initializing db schema")
				ret = makeSchema(s.sex, icmd.getMessage())
			case mgrSyncNodesOp:
				slogger.Infof("syncing node configs")
				ret = syncNodes(s.sex, icmd.getMessage())
			case mgrGetNodeOp:
				slogger.Infof("getting node name")
				nMsg := icmd.getMessage().(nodeMessage)
				nodeIP := net.ParseIP(nMsg.getNodeIP())
				node, err := s.cache.LookupNode(nodeIP)
				if err == errNoNode {
					slogger.Infof("Node cache miss. Looking up node for IP: %s", nMsg.getNodeIP())
					ret = getNode(s.sex, icmd.getMessage())
					if ret.Error() == nil {
						s.cache.addNode(ret.(nodeReply).getNode())
					} else {
						slogger.Errorf("Error getting node: %s", ret.Error())
					}
				} else {
					ret = newNodeReply(node, nil)
				}
			case mgrGetTableOp:
				tMsg := icmd.getMessage().(tableMessage)
				colIP := tMsg.getColIP()
				date := tMsg.getDate()
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
		nodeip := tMsg.getColIP()
		date := tMsg.getDate()

		node, err := s.cache.LookupNode(net.ParseIP(nodeip))
		if err != nil {
			return newReply(err)
		}
		ddm := time.Duration(node.duration) * time.Minute
		start := date.Truncate(ddm)

		tName := genTableName(node.name, date, node.duration)
		cMsg := newCapTableMessage(tName, node.name, start, start.Add(ddm))
		nsout := createCaptureTable(s.sex, cMsg).(capTableReply)
		if err := nsout.Error(); err != nil {
			return newReply(fmt.Errorf("makeCapTable: %s", err))
		}
		s, e := nsout.getDates()
		res = newTableReply(nsout.getName(), s, e, node, nil)
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
	s.daemonWG.Wait()
}

func (s *schemaMgr) checkSchema() (bool, error) {
	cmdin := newSchemaMessage(s.getCommonMessage(), mgrCheckSchemaOp)
	s.iChan <- cmdin
	sreply := <-s.oChan
	return sreply.Error() == nil, sreply.Error()
}

func (s *schemaMgr) makeSchema() error {
	cmdin := newSchemaMessage(s.getCommonMessage(), mgrInitSchemaOp)
	s.iChan <- cmdin
	sreply := <-s.oChan
	return sreply.Error()
}

func (s *schemaMgr) syncNodes(knownNodes map[string]config.NodeConfig) (map[string]config.NodeConfig, error) {
	nMsg := newNodesMessage(knownNodes)
	s.setMessageTables(nMsg)

	cmdin := newSchemaMessage(nMsg, mgrSyncNodesOp)
	s.iChan <- cmdin
	sreply := <-s.oChan
	nRep := sreply.(nodesReply)

	return nRep.getNodes(), nRep.Error()
}

func (s *schemaMgr) getTable(ipstr string, date time.Time) (string, time.Time, time.Time, error) {
	tMsg := newTableMessage(ipstr, date)
	s.setMessageTables(tMsg)

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

func (s *schemaMgr) getNode(nodeName string, nodeIP string) (*node, error) {
	nMsg := newNodeMessage(nodeName, nodeIP)
	s.setMessageTables(nMsg)

	cmdin := newSchemaMessage(nMsg, mgrGetNodeOp)
	s.iChan <- cmdin
	sreply := <-s.oChan
	nRep := sreply.(nodeReply)
	return nRep.getNode(), nRep.Error()
}

func (s *schemaMgr) LookupTable(nodeIP net.IP, t time.Time) (string, error) {
	tName, _, _, err := s.getTable(nodeIP.String(), t)
	return tName, err
}

func (s *schemaMgr) LookupNode(nodeIP net.IP) (*node, error) {
	n, err := s.getNode("", nodeIP.String())
	return n, err
}
