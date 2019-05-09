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
	sLogger = util.NewLogger("system", "schema")
)

const (
	mgrInitSchemaOp = iota
	mgrCheckSchemaOp
	mgrGetNodeOp
	mgrSyncNodesOp
	mgrGetTableOp
)

type schemaMgr struct {
	req      chan schemaMessage
	resp     chan CommonReply
	sEx      SessionExecutor // This should not be a AtomicSQLExecutor
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
func newSchemaMgr(sEx SessionExecutor, main, node, entity string) *schemaMgr {
	sm := &schemaMgr{
		req:         make(chan schemaMessage),
		resp:        make(chan CommonReply),
		sEx:         sEx,
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
	defer sLogger.Infof("Schema manager closed successfully")
	defer close(s.resp)
	defer s.daemonWG.Done()

	for {
		select {
		case cmd, ok := <-s.req:
			if !ok {
				return
			}
			var ret CommonReply

			switch cmd.getType() {
			case mgrCheckSchemaOp:
				sLogger.Infof("checking correctness of db schema")
				ret = checkSchema(s.sEx, cmd.getMessage())
			case mgrInitSchemaOp:
				sLogger.Infof("initializing db schema")
				ret = makeSchema(s.sEx, cmd.getMessage())
			case mgrSyncNodesOp:
				sLogger.Infof("syncing node configs")
				ret = syncNodes(s.sEx, cmd.getMessage())
			case mgrGetNodeOp:
				sLogger.Infof("getting node name")
				nMsg := cmd.getMessage().(nodeMessage)
				nodeIP := net.ParseIP(nMsg.getNodeIP())
				node, err := s.cache.LookupNode(nodeIP)
				if err == errNoNode {
					sLogger.Infof("Node cache miss. Looking up node for IP: %s", nMsg.getNodeIP())
					ret = getNode(s.sEx, cmd.getMessage())
					if ret.Error() == nil {
						s.cache.addNode(ret.(nodeReply).getNode())
					} else {
						sLogger.Errorf("Error getting node: %s", ret.Error())
					}
				} else {
					ret = newNodeReply(node, nil)
				}
			case mgrGetTableOp:
				tMsg := cmd.getMessage().(tableMessage)
				colIP := tMsg.getColIP()
				date := tMsg.getDate()
				tName, err := s.cache.LookupTable(net.ParseIP(colIP), date)
				if err == errNoNode {
					ret = newReply(err)
				} else if err == errNoTable {
					sLogger.Infof("Table cache miss. Creating table for col: %s date: %s", colIP, date)
					ret = s.makeCapTable(tMsg)
					if ret.Error() != nil {
						sLogger.Errorf("schemaMgr: %s", ret.Error())
					} else {
						s.cache.addTable(net.ParseIP(colIP), date)
					}
				} else {
					ret = newTableReply(tName, time.Now(), time.Now(), nil, nil)
				}
			default:
				ret = newReply(fmt.Errorf("unhandled schema manager command:%+v", cmd))
			}

			// Send the result back on the channel
			s.resp <- ret
		}
	}
}

func (s *schemaMgr) makeCapTable(msg CommonMessage) CommonReply {
	res := getTable(s.sEx, msg).(tableReply)
	var node *node

	if err := res.Error(); err == errNoTable {
		tMsg := msg.(tableMessage)
		nodeIP := tMsg.getColIP()
		date := tMsg.getDate()

		node, err := s.cache.LookupNode(net.ParseIP(nodeIP))
		if err != nil {
			return newReply(err)
		}
		dur := time.Duration(node.duration) * time.Minute
		start := date.Truncate(dur)

		tName := genTableName(node.name, date, node.duration)
		cMsg := newCapTableMessage(tName, node.name, start, start.Add(dur))
		capRep := createCaptureTable(s.sEx, cMsg).(capTableReply)
		if err := capRep.Error(); err != nil {
			return newReply(fmt.Errorf("makeCapTable: %s", err))
		}

		start, end := capRep.getDates()
		res = newTableReply(capRep.getName(), start, end, node, nil)
	} else if err != nil {
		return newReply(fmt.Errorf("makeCapTable: %s", err))
	} else {
		// we have a node table already and res contains the correct vaules to be added in the cache
		node = res.getNode()
		s.cache.addNode(node)
	}
	return res
}

// Below this are the schema manager client functions, called by the session streams

// This doesn't need a dedicated close channel. With the way we use it,
// none of the other interface methods will be called after stop is called.
func (s *schemaMgr) stop() {
	close(s.req)
	s.daemonWG.Wait()
}

func (s *schemaMgr) checkSchema() error {
	cmdin := newSchemaMessage(s.getCommonMessage(), mgrCheckSchemaOp)
	s.req <- cmdin
	sreply := <-s.resp
	return sreply.Error()
}

func (s *schemaMgr) makeSchema() error {
	cmdin := newSchemaMessage(s.getCommonMessage(), mgrInitSchemaOp)
	s.req <- cmdin
	sreply := <-s.resp
	return sreply.Error()
}

func (s *schemaMgr) syncNodes(knownNodes map[string]config.NodeConfig) (map[string]config.NodeConfig, error) {
	nMsg := newNodesMessage(knownNodes)
	s.setMessageTables(nMsg)

	cmdin := newSchemaMessage(nMsg, mgrSyncNodesOp)
	s.req <- cmdin
	sreply := <-s.resp
	nRep := sreply.(nodesReply)

	return nRep.getNodes(), nRep.Error()
}

func (s *schemaMgr) getTable(ipStr string, date time.Time) (name string, start time.Time, end time.Time, err error) {
	tMsg := newTableMessage(ipStr, date)
	s.setMessageTables(tMsg)

	cmdin := newSchemaMessage(tMsg, mgrGetTableOp)
	s.req <- cmdin
	sreply := <-s.resp

	if sreply.Error() != nil {
		return "", time.Time{}, time.Time{}, sreply.Error()
	}

	tRep := sreply.(tableReply)
	err = tRep.Error()
	name = tRep.getName()
	start, end = tRep.getDates()

	return
}

func (s *schemaMgr) getNode(nodeName string, nodeIP string) (*node, error) {
	nMsg := newNodeMessage(nodeName, nodeIP)
	s.setMessageTables(nMsg)

	cmdin := newSchemaMessage(nMsg, mgrGetNodeOp)
	s.req <- cmdin
	sreply := <-s.resp
	nRep := sreply.(nodeReply)
	return nRep.getNode(), nRep.Error()
}

// LookupTable allows schemaMgr to adhere to the tableCache interface
func (s *schemaMgr) LookupTable(nodeIP net.IP, t time.Time) (string, error) {
	tName, _, _, err := s.getTable(nodeIP.String(), t)
	return tName, err
}

// LookupNode allows schemaMgr to adhere to the tableCache interface
func (s *schemaMgr) LookupNode(nodeIP net.IP) (*node, error) {
	n, err := s.getNode("", nodeIP.String())
	return n, err
}
