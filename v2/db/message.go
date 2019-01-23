package db

import (
	"github.com/CSUNetSec/bgpmon/v2/config"
	"time"
)

// This file is meant to define a heirarchy of messages
// These are meant to be a substitute for the sqlIn/sqlOut
// structures, which have become bloated

type CommonMessage interface {
	GetMainTable() string // This table holds the names of all other tables created
	GetNodeTable() string // This holds all info on the collectors
}

type msg struct{}

func (m msg) GetMainTable() string { return "dbs" }
func (m msg) GetNodeTable() string { return "nodes" }

func NewMessage() CommonMessage {
	return msg{}
}

type nodesMessage struct {
	CommonMessage
	nodes map[string]config.NodeConfig
}

func NewNodesMessage(nodes map[string]config.NodeConfig) nodesMessage {
	return nodesMessage{CommonMessage: NewMessage(), nodes: nodes}
}

func (n nodesMessage) GetNodes() map[string]config.NodeConfig {
	return n.nodes
}

type nodeMessage struct {
	CommonMessage
	nodeName string
	nodeIP   string
}

func NewNodeMessage(name, ip string) nodeMessage {
	return nodeMessage{nodeName: name, nodeIP: ip}
}

func (n nodeMessage) GetNodeName() string {
	return n.nodeName
}

func (n nodeMessage) GetNodeIP() string {
	return n.nodeIP
}

type capTableMessage struct {
	CommonMessage
	tableName string
	tableCol  string
	sDate     time.Time
	eDate     time.Time
}

func NewCapTableMessage(name, col string, start, end time.Time) capTableMessage {
	return capTableMessage{CommonMessage: NewMessage(), tableName: name, tableCol: col, sDate: start, eDate: end}
}

func (c capTableMessage) GetTableName() string {
	return c.tableName
}

func (c capTableMessage) GetTableCol() string {
	return c.tableCol
}

func (c capTableMessage) GetDates() (time.Time, time.Time) {
	return c.sDate, c.eDate
}

type tableMessage struct {
	CommonMessage
	colDate collectorDate
}

func NewTableMessage(colDate collectorDate) tableMessage {
	return tableMessage{CommonMessage: NewMessage(), colDate: colDate}
}

func (t tableMessage) GetColDate() collectorDate {
	return t.colDate
}

type CommonReply interface {
	GetErr() error
}

type rpy struct{ err error }

func (r rpy) GetErr() error { return r.err }

func NewReply(e error) CommonReply {
	return rpy{err: e}
}

type nodesReply struct {
	CommonReply
	nodes map[string]config.NodeConfig
}

func NewNodesReply(nodes map[string]config.NodeConfig, err error) nodesReply {
	return nodesReply{CommonReply: NewReply(err), nodes: nodes}
}

func (n nodesReply) GetNodes() map[string]config.NodeConfig {
	return n.nodes
}

type nodeReply struct {
	CommonReply
	node *node
}

func NewNodeReply(n *node, err error) nodeReply {
	return nodeReply{CommonReply: NewReply(err), node: n}
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

func NewCapTableReply(name, ip string, sDate, eDate time.Time, err error) capTableReply {
	return capTableReply{CommonReply: NewReply(err), name: name, ip: ip, sDate: sDate, eDate: eDate}
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

func NewTableReply(name string, start, end time.Time, n *node, err error) tableReply {
	return tableReply{CommonReply: NewReply(err), name: name, sDate: start, eDate: end, node: n}
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
