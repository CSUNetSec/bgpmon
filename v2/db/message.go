package db

import (
	"time"

	"encoding"

	"github.com/CSUNetSec/bgpmon/v2/config"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
)

//CommonMessage is a basic interface which allows the getting and setting of the the main
//and node tables that are usually involved in most db operations
type CommonMessage interface {
	GetMainTable() string // This table holds the names of all other tables created
	GetNodeTable() string // This holds all info on the collectors
	SetMainTable(string)
	SetNodeTable(string)
}

type msg struct {
	mainTable string
	nodeTable string
}

func (m *msg) GetMainTable() string  { return m.mainTable }
func (m *msg) GetNodeTable() string  { return m.nodeTable }
func (m *msg) SetMainTable(n string) { m.mainTable = n }
func (m *msg) SetNodeTable(n string) { m.nodeTable = n }

func newMessage() CommonMessage {
	return &msg{mainTable: "dbs", nodeTable: "nodes"}
}

func newCustomMessage(main, node string) CommonMessage {
	return &msg{mainTable: main, nodeTable: node}
}

type nodesMessage struct {
	CommonMessage
	nodes map[string]config.NodeConfig
}

//newNodesMessage creates a nodes message that will pass to the db
//a map of NodeConfigs. the string in the map should be the textual IP
//of the node that will have that config.
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
	sDate     time.Time
	eDate     time.Time
}

func newCapTableMessage(name, col string, start, end time.Time) capTableMessage {
	return capTableMessage{CommonMessage: newMessage(), tableName: name, tableCol: col, sDate: start, eDate: end}
}

func (c capTableMessage) getTableName() string {
	return c.tableName
}

func (c capTableMessage) getTableCol() string {
	return c.tableCol
}

func (c capTableMessage) getDates() (time.Time, time.Time) {
	return c.sDate, c.eDate
}

type tableMessage struct {
	CommonMessage
	colDate collectorDate
}

func newTableMessage(colDate collectorDate) tableMessage {
	return tableMessage{CommonMessage: newMessage(), colDate: colDate}
}

func (t tableMessage) getColDate() collectorDate {
	return t.colDate
}

type captureMessage struct {
	CommonMessage
	tableName string
	capture   *pb.WriteRequest
}

func newCaptureMessage(name string, cap *pb.WriteRequest) captureMessage {
	return captureMessage{CommonMessage: newMessage(), tableName: name, capture: cap}
}

func (c captureMessage) getTableName() string {
	return c.tableName
}

func (c captureMessage) getCapture() *pb.WriteRequest {
	return c.capture
}

//CommonReply is an interface that provides the Error interface
type CommonReply interface {
	Error() error
}

type rpy struct{ err error }

func (r rpy) Error() error { return r.err }

func newReply(e error) CommonReply {
	return rpy{err: e}
}

type nodesReply struct {
	CommonReply
	nodes map[string]config.NodeConfig
}

func newNodesReply(nodes map[string]config.NodeConfig, err error) nodesReply {
	return nodesReply{CommonReply: newReply(err), nodes: nodes}
}

func (n nodesReply) GetNodes() map[string]config.NodeConfig {
	return n.nodes
}

type nodeReply struct {
	CommonReply
	node *node
}

func newNodeReply(n *node, err error) nodeReply {
	return nodeReply{CommonReply: newReply(err), node: n}
}

func (n nodeReply) GetNode() *node {
	return n.node
}

type capTableReply struct {
	CommonReply
	name  string
	ip    string
	sDate time.Time
	eDate time.Time
}

func newCapTableReply(name, ip string, sDate, eDate time.Time, err error) capTableReply {
	return capTableReply{CommonReply: newReply(err), name: name, ip: ip, sDate: sDate, eDate: eDate}
}

func (c capTableReply) GetName() string {
	return c.name
}

func (c capTableReply) GetIP() string {
	return c.ip
}

func (c capTableReply) GetDates() (time.Time, time.Time) {
	return c.sDate, c.eDate
}

type tableReply struct {
	CommonReply
	name  string
	sDate time.Time
	eDate time.Time
	node  *node
}

func newTableReply(name string, start, end time.Time, n *node, err error) tableReply {
	return tableReply{CommonReply: newReply(err), name: name, sDate: start, eDate: end, node: n}
}

func (t tableReply) GetName() string {
	return t.name
}

func (t tableReply) GetDates() (time.Time, time.Time) {
	return t.sDate, t.eDate
}

func (t tableReply) GetNode() *node {
	return t.node
}

type captureReply struct {
	CommonReply
	captsz  int
	capblob []byte
}

func newCaptureReply(blob []byte, err error) captureReply {
	return captureReply{
		CommonReply: newReply(err),
		captsz:      len(blob),
		capblob:     blob,
	}
}

func (c captureReply) MarshalBinary() ([]byte, error) {
	return c.capblob, nil
}

//SerializableReply is a CommonReply that provides Error as well as
//something that can be Marshaled to Binary
type SerializableReply interface {
	CommonReply
	encoding.BinaryMarshaler
}
