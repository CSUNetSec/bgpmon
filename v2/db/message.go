package db

// This file is meant to define a heirarchy of messages
// These are meant to be a substitute for the sqlIn/sqlOut
// structures, which have become bloated

type CommonMessage interface {
	GetMainTable() string   // This table holds the names of all other tables created
	GetNodeTable() string   // This holds all info on the collectors
	GetTargetTable() string // This is the "target" table of the message, the one the message is operating on
}

type msg struct{}

func (m msg) GetMainTable() string   { return "dbs" }
func (m msg) GetNodeTable() string   { return "nodes" }
func (m msg) GetTargetTable() string { return "" }

func NewMessage() CommonMessage {
	return msg{}
}

type CommonReply interface {
	GetErr() error
}

type rpy struct{ err error }

func (r rpy) GetErr() error { return r.err }

func NewReply(e error) CommonReply {
	return rpy{err: e}
}
