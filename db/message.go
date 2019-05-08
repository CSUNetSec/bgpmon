package db

import (
	"net"
	"time"

	"github.com/CSUNetSec/bgpmon/config"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
)

// CommonMessage is a basic interface which allows the getting and setting of the the main
// and node tables that are usually involved in most db operations.
type CommonMessage interface {
	// This table holds the names of all other tables created.
	GetMainTable() string

	// This holds all info on the collectors.
	GetNodeTable() string

	// This holds info on customers or anyone interested in the stored
	// BGP data.
	GetEntityTable() string

	SetMainTable(string)
	SetNodeTable(string)
	SetEntityTable(string)
}

type msg struct {
	mainTable   string
	nodeTable   string
	entityTable string
}

func (m *msg) GetMainTable() string   { return m.mainTable }
func (m *msg) GetNodeTable() string   { return m.nodeTable }
func (m *msg) GetEntityTable() string { return m.entityTable }

func (m *msg) SetMainTable(n string)   { m.mainTable = n }
func (m *msg) SetNodeTable(n string)   { m.nodeTable = n }
func (m *msg) SetEntityTable(n string) { m.entityTable = n }

func newMessage() CommonMessage {
	return &msg{
		mainTable:   defaultMainTable,
		nodeTable:   defaultNodeTable,
		entityTable: defaultEntityTable,
	}
}

func newCustomMessage(main, node, entity string) CommonMessage {
	return &msg{
		mainTable:   main,
		nodeTable:   node,
		entityTable: entity,
	}
}

type nodesMessage struct {
	CommonMessage
	nodes map[string]config.NodeConfig
}

// newNodesMessage creates a nodes message that will pass to the db
// a map of NodeConfigs. The string in the map should be the textual IP
// of the node that will have that config.
func newNodesMessage(nodes map[string]config.NodeConfig) nodesMessage {
	return nodesMessage{CommonMessage: newMessage(), nodes: nodes}
}

func (n nodesMessage) getNodes() map[string]config.NodeConfig {
	return n.nodes
}

type nodeMessage struct {
	CommonMessage
	nodeName string
	nodeIP   string
}

func newNodeMessage(name, ip string) nodeMessage {
	return nodeMessage{CommonMessage: newMessage(), nodeName: name, nodeIP: ip}
}

func (n nodeMessage) getNodeName() string {
	return n.nodeName
}

func (n nodeMessage) getNodeIP() string {
	return n.nodeIP
}

type capTableMessage struct {
	CommonMessage
	tableName string
	tableCol  string
	start     time.Time
	end       time.Time
}

func newCapTableMessage(name, col string, start, end time.Time) capTableMessage {
	return capTableMessage{CommonMessage: newMessage(), tableName: name, tableCol: col, start: start, end: end}
}

func (c capTableMessage) getTableName() string {
	return c.tableName
}

func (c capTableMessage) getTableCol() string {
	return c.tableCol
}

func (c capTableMessage) getDates() (start time.Time, end time.Time) {
	return c.start, c.end
}

type tableMessage struct {
	CommonMessage
	colIP   string
	colDate time.Time
}

func newTableMessage(ip string, date time.Time) tableMessage {
	return tableMessage{CommonMessage: newMessage(), colIP: ip, colDate: date}
}

func (t tableMessage) getColIP() string {
	return t.colIP
}

func (t tableMessage) getDate() time.Time {
	return t.colDate
}

type captureMessage struct {
	CommonMessage
	tableName string
	capture   *pb.BGPCapture
}

func newCaptureMessage(name string, wr *pb.WriteRequest) captureMessage {
	var cap *pb.BGPCapture
	// This is necessary to unwrap the capture from the WriteRequest
	if wr != nil {
		cap = wr.GetBgpCapture()
	}
	return captureMessage{CommonMessage: newMessage(), tableName: name, capture: cap}
}

func (c captureMessage) getTableName() string {
	return c.tableName
}

func (c captureMessage) getCapture() *pb.BGPCapture {
	return c.capture
}

type schemaMessage struct {
	CommonMessage
	cmdType int
}

func newSchemaMessage(par CommonMessage, cmd int) schemaMessage {
	return schemaMessage{CommonMessage: par, cmdType: cmd}
}

func (s schemaMessage) getMessage() CommonMessage {
	return s.CommonMessage
}

func (s schemaMessage) getType() int {
	return s.cmdType
}

// CommonReply will wrap any data that needs to be returned by a dbOp. Each
// CommonReply has one thing in common, the ability to return an error.
type CommonReply interface {
	Error() error
}

type reply struct{ err error }

func (r reply) Error() error { return r.err }

func newReply(e error) CommonReply {
	return reply{err: e}
}

type nodesReply struct {
	CommonReply
	nodes map[string]config.NodeConfig
}

func newNodesReply(nodes map[string]config.NodeConfig, err error) nodesReply {
	return nodesReply{CommonReply: newReply(err), nodes: nodes}
}

func (n nodesReply) getNodes() map[string]config.NodeConfig {
	return n.nodes
}

type nodeReply struct {
	CommonReply
	node *node
}

func newNodeReply(n *node, err error) nodeReply {
	return nodeReply{CommonReply: newReply(err), node: n}
}

func (n nodeReply) getNode() *node {
	return n.node
}

type capTableReply struct {
	CommonReply
	name  string
	ip    string
	start time.Time
	end   time.Time
}

func newCapTableReply(name, ip string, start, end time.Time, err error) capTableReply {
	return capTableReply{CommonReply: newReply(err), name: name, ip: ip, start: start, end: end}
}

func (c capTableReply) getName() string {
	return c.name
}

func (c capTableReply) getIP() string {
	return c.ip
}

func (c capTableReply) getDates() (start time.Time, end time.Time) {
	return c.start, c.end
}

type tableReply struct {
	CommonReply
	name  string
	start time.Time
	end   time.Time
	node  *node
}

func newTableReply(name string, start, end time.Time, n *node, err error) tableReply {
	return tableReply{CommonReply: newReply(err), name: name, start: start, end: end, node: n}
}

func (t tableReply) getName() string {
	return t.name
}

func (t tableReply) getDates() (start time.Time, end time.Time) {
	return t.start, t.end
}

func (t tableReply) getNode() *node {
	return t.node
}

type getCapMessage struct {
	capTableMessage
}

func newGetCapMessage(rf ReadFilter) getCapMessage {
	return getCapMessage{newCapTableMessage("", rf.collector, rf.start, rf.end)}
}

type getCapReply struct {
	CommonReply
	cap *Capture
}

func (c *getCapReply) getCapture() *Capture {
	return c.cap
}

func newGetCapReply(cap *Capture, err error) *getCapReply {
	return &getCapReply{
		CommonReply: newReply(err),
		cap:         cap,
	}
}

type getPrefixReply struct {
	CommonReply
	net *net.IPNet
}

func (gpr *getPrefixReply) getPrefix() *net.IPNet {
	return gpr.net
}

func newGetPrefixReply(pref string, msgErr error) *getPrefixReply {
	_, net, pErr := net.ParseCIDR(pref)

	// This should prefer the original error passed, if it's populated.
	// If it's not populated, it will fall back to the parse error.
	var err error
	if msgErr != nil {
		err = msgErr
	} else {
		err = pErr
	}

	return &getPrefixReply{
		CommonReply: newReply(err),
		net:         net,
	}
}

type entityMessage struct {
	CommonMessage
	entity *Entity
}

func (em *entityMessage) getEntity() *Entity {
	return em.entity
}

func newEntityMessage(ent *Entity) *entityMessage {
	return &entityMessage{CommonMessage: newMessage(), entity: ent}
}

type entityReply struct {
	CommonReply
	entity *Entity
}

func (er *entityReply) getEntity() *Entity {
	return er.entity
}

func newEntityReply(e *Entity, err error) *entityReply {
	return &entityReply{CommonReply: newReply(err), entity: e}
}

type filterMessage struct {
	CommonMessage

	rf ReadFilter
}

func (fm *filterMessage) getFilter() ReadFilter {
	return fm.rf
}

func newFilterMessage(rf ReadFilter) *filterMessage {
	return &filterMessage{CommonMessage: newMessage(), rf: rf}
}
