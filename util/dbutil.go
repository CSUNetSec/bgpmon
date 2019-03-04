package util

import (
	"database/sql"
	"database/sql/driver"
	"github.com/CSUNetSec/bgpmon/config"
	"github.com/lib/pq"
	"net"
	"time"
)

// SumNodeConfs combines two maps of NodeConfigs, preferring the first in case of overlap
func SumNodeConfs(confnodes, dbnodes map[string]config.NodeConfig) map[string]config.NodeConfig {
	ret := make(map[string]config.NodeConfig)
	for k1, v1 := range confnodes {
		ret[k1] = v1 //if it exists in config, prefer config
	}
	for k2, v2 := range dbnodes {
		if _, ok := confnodes[k2]; ok { // exists in config, so ignore it
			continue
		}
		//does not exist in config, so add it in ret as it is in the db
		ret[k2] = v2
	}
	return ret
}

//helper function that checks if t is in the [t1,t2) range
func inTimeRange(t, t1, t2 time.Time) bool {
	if t1.Equal(t) {
		return true
	}
	if t1.Before(t) && t.Before(t2) {
		return true
	}
	return false
}

// SQLExecutor is a wrapper around sql.Tx, sql.Db, and others we implement. It represents
// something that can execute queries on a database.
type SQLExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// SQLErrorExecutor is a SQLExecutor with a persistent error, useful in tx
type SQLErrorExecutor interface {
	SQLExecutor
	SetError(error)
}

//PrefixesToPQArray handles a strange case where protobuf deserialize an array element of nil as "<nil>"
//and that kills the db insert statement cause it can't make it into a cidr.
func PrefixesToPQArray(n []*net.IPNet) interface {
	driver.Valuer
	sql.Scanner
} {
	if n == nil || len(n) == 0 {
		return nil //database will accept NULL on this field
	}

	ret := make([]string, len(n))
	for ct := range n {
		ret[ct] = n[ct].String()
		if ret[ct] == "" || ret[ct] == "<nil>" {
			//lol someone (protobuf!?) makes this string be <nil>. change it
			//to a database default value
			ret[ct] = "0.0.0.0/0"
		}
	}
	return pq.Array(ret)
}
