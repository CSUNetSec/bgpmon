// Package db is responsible for the communication between the database backends and the bgpmon daemon.
// It abstracts database operations through types that are named Executors. Executors allow for one off, or
// transactional operations, that can be timed out using a context, and can be database unware by getting
// the appropriate SQL statement depending what type of database exists on the back.
package db

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"time"

	"github.com/CSUNetSec/bgpmon/config"
	"github.com/CSUNetSec/bgpmon/util"
)

// This block holds the currently supported database backends.
const (
	postgres = iota
)

// This block holds the supported operations on the databases.
const (
	connectNoSSLOp       = "connectNoSSL"
	connectSSLOp         = "connectSSL"
	checkSchemaOp        = "checkschema"
	selectNodeOp         = "selectNodeTmpl"
	insertNodeOp         = "insertNodeTmpl"
	insertMainTableOp    = "insertMainTableTmpl"
	makeMainTableOp      = "makeMainTableTmpl"
	selectTableOp        = "selectTableTmpl"
	makeNodeTableOp      = "makeNodeTableTmpl"
	makeCaptureTableOp   = "makeCaptureTableTmpl"
	insertCaptureTableOp = "insertCaptureTableTmpl"
	getCaptureTablesOp   = "getCaptureTablesTmpl"
	getCaptureBinaryOp   = "getCaptureBinaryTmpl"
)

// dbops associates every generic database operation with an array that holds the correct SQL statements
// for each type of database. The index in the value array is controlled by the database const.
var dbops = map[string][]string{
	connectNoSSLOp: {
		// postgres
		`user=%s password=%s dbname=%s host=%s sslmode=disable`,
	},
	connectSSLOp: {
		// postgres
		`user=%s password=%s dbname=%s host=%s`,
	},
	checkSchemaOp: {
		// postgres
		`SELECT EXISTS (
		   SELECT *
		   FROM   information_schema.tables
		   WHERE  table_name = $1
		 );`,
	},
	selectNodeOp: {
		// postgres
		`SELECT name, ip, isCollector, tableDumpDurationMinutes, description, coords, address FROM %s;`,
	},
	insertNodeOp: {
		// postgres
		`INSERT INTO %s (name, ip, isCollector, tableDumpDurationMinutes, description, coords, address) 
		   VALUES ($1, $2, $3, $4, $5, $6, $7)
		   ON CONFLICT (ip) DO UPDATE SET name=EXCLUDED.name, isCollector=EXCLUDED.isCollector, 
		     tableDumpDurationMinutes=EXCLUDED.tableDumpDurationMinutes,
		     description=EXCLUDED.description, coords=EXCLUDED.coords, address=EXCLUDED.address;`,
	},
	makeMainTableOp: {
		// postgres
		`CREATE TABLE IF NOT EXISTS %s (
		   dbname varchar PRIMARY KEY,
	           collector varchar NOT NULL,
	           dateFrom timestamp NOT NULL,
	           dateTo timestamp NOT NULL
                 );`,
	},
	insertMainTableOp: {
		// postgres
		`INSERT INTO %s (dbname, collector, dateFrom, dateTo) VALUES ($1, $2, $3, $4);`,
	},
	makeCaptureTableOp: {
		// postgres
		`CREATE TABLE IF NOT EXISTS %s (
		   update_id BIGSERIAL PRIMARY KEY NOT NULL, 
		   timestamp timestamp NOT NULL,
		   collector_ip inet NOT NULL, 
		   peer_ip inet NOT NULL, 
		   as_path integer[] DEFAULT '{}'::integer[],
		   next_hop inet DEFAULT '0.0.0.0'::inet,
		   origin_as integer DEFAULT '0'::integer,
		   adv_prefixes cidr[] DEFAULT '{}'::cidr[],
		   wdr_prefixes cidr[] DEFAULT '{}'::cidr[],
		   protomsg bytea NOT NULL);`,
	},
	// This template shouldn't need VALUES, because those will be provided by the buffer
	insertCaptureTableOp: {
		// postgres
		`INSERT INTO %s (timestamp, collector_ip, peer_ip, as_path, next_hop, origin_as, adv_prefixes, wdr_prefixes, protomsg) VALUES `,
	},
	selectTableOp: {
		// postgres
		`SELECT dbname, collector, dateFrom, dateTo, tableDumpDurationMinutes FROM %s,%s 
		 WHERE dateFrom <= $1 AND dateTo > $1 AND ip = $2;`,
	},
	makeNodeTableOp: {
		// postgres
		`CREATE TABLE IF NOT EXISTS %s (
		   ip varchar PRIMARY KEY,
		   name varchar NOT NULL, 
		   isCollector boolean NOT NULL,
		   tableDumpDurationMinutes integer NOT NULL,
		   description varchar NOT NULL,
		   coords varchar NOT NULL,
		   address varchar NOT NULL
	         );`,
	},
	getCaptureTablesOp: {
		//postgres
		`SELECT dbname FROM %s WHERE collector=$1 AND dateFrom>=$2 AND dateTo<$3;`,
	},
	getCaptureBinaryOp: {
		//postgres
		`SELECT update_id, origin_as, protomsg FROM %s;`,
	},
}

// dblogger is the logger for the database subsystem.
var (
	dblogger = util.NewLogger("system", "db")
)

// Dber is an interface that returns a reference to the underlying *sql.DB
type Dber interface {
	Db() *sql.DB
}

// TimeoutDber composes Dber and GetTimeouter to return timeout duration for dbops
type TimeoutDber interface {
	Dber
	util.GetTimeouter
}

type getdboper interface {
	getdbop(string) string
}

type dbOper struct {
	t int
}

func newPostgressDbOper() *dbOper {
	return &dbOper{
		t: postgres,
	}
}

// Gets the specific db op string from the static table declared in db.go
// for the appropriate dbType that was populated when the correct newSession was called.
// Termintates the application on errors.
// implementing the getdboper interface in db.go
func (d *dbOper) getdbop(a string) string {
	var (
		sslice []string
		exists bool
	)
	sslice, exists = dbops[a]
	if !exists {
		dblogger.Fatalf("nx db op name:%s requested.", a)
	}
	if len(sslice)-1 < d.t {
		dblogger.Fatalf("dbop:%s for this db type not populated", a)
	}
	return sslice[d.t]
}

// SessionExecutor wraps an util.SQLExecutor with getdboper
type SessionExecutor struct {
	util.SQLExecutor
	getdboper
}

func newSessionExecutor(ex util.SQLExecutor, oper getdboper) SessionExecutor {
	return SessionExecutor{SQLExecutor: ex, getdboper: oper}
}

func (s SessionExecutor) getExecutor() util.SQLExecutor {
	return s.SQLExecutor
}

func (s SessionExecutor) getOper() getdboper {
	return s.getdboper
}

// a wrapper of a sql.Tx that is able to accept multiple
// db ops and run them in the same tx.
// it will implement the SQLAtomicExectutor interface and choose
// where to apply the sql function depending on how it was constructed.
type ctxTx struct {
	doTx bool
	tx   *sql.Tx
	db   *sql.DB
	cf   context.CancelFunc
	ctx  context.Context
}

// newCtxExecutor creates a new ctxTx for that operation which implements the
// SQLAtomicExecutor interface. The argument passed instructs it to either
// do it on a transaction if true, or on the normal DB connection if false.
func newCtxExecutor(tdb TimeoutDber, doTx bool) (*ctxTx, error) {
	var (
		tx  *sql.Tx
		err error
		db  *sql.DB
	)

	db = tdb.Db()
	ctx, cf := context.WithTimeout(context.Background(), tdb.GetTimeout())
	if doTx {
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			cf()
			return nil, err
		}
	}

	return &ctxTx{
		doTx: doTx,
		tx:   tx,
		cf:   cf,
		ctx:  ctx,
		db:   db,
	}, nil
}

// Exec makes the CtxExecutor conform to the the sql.Db semantics.
func (c *ctxTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	if c.doTx && c.tx != nil {
		return c.tx.ExecContext(c.ctx, query, args...)
	}
	return c.db.ExecContext(c.ctx, query, args...)
}

// Query makes the CtxExecutor conform to the the sql.Db semantics.
func (c *ctxTx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if c.doTx && c.tx != nil {
		return c.tx.QueryContext(c.ctx, query, args...)
	}
	return c.db.QueryContext(c.ctx, query, args...)
}

// QueryRow makes the CtxExecutor conform to the the sql.Db semantics.
func (c *ctxTx) QueryRow(query string, args ...interface{}) *sql.Row {
	if c.doTx && c.tx != nil {
		return c.tx.QueryRowContext(c.ctx, query, args...)
	}
	return c.db.QueryRowContext(c.ctx, query, args...)
}

// Commit makes the CtxExecutor conform the sq.Tx semantics.
func (c *ctxTx) Commit() error {
	defer c.cf()
	if !c.doTx || c.tx == nil {
		return fmt.Errorf("ctxTx can't be committed when not using a transaction")
	}

	return c.tx.Commit()
}

// Rollback makes the CtxExecutor conform the sq.Tx semantics.
func (c *ctxTx) Rollback() error {
	defer c.cf()
	if !c.doTx || c.tx == nil {
		return fmt.Errorf("ctxTx can't be rolled back when not using a transaction")
	}

	return c.tx.Rollback()
}

// node is a representation of a machine that is stored in the database using this fields.
// a node can be either a collector or a peer, and in case of being a collector it is used
// to generate the table names that data collected by it are stored. it should be also geolocated.
// known nodes can be supplied by the config file.
type node struct {
	nodeName      string
	nodeIP        string
	nodeCollector bool
	nodeDuration  int
	nodeDescr     string
	nodeCoords    string
	nodeAddress   string
}

// newNode creates an empty node.
func newNode() *node {
	return &node{}
}

// nodeConfigFromNode creates a node configuration from a node.
func (a *node) nodeConfigFromNode() config.NodeConfig {
	return config.NodeConfig{
		Name:                a.nodeName,
		IP:                  a.nodeIP,
		IsCollector:         a.nodeCollector,
		DumpDurationMinutes: a.nodeDuration,
		Description:         a.nodeDescr,
		Coords:              a.nodeCoords,
		Location:            a.nodeAddress,
	}
}

// tableCache provides functions to lookup caches for tables and nodes.
type tableCache interface {
	LookupTable(net.IP, time.Time) (string, error)
	LookupNode(net.IP) (*node, error)
}

// dbCache implements the tableCache interface.
type dbCache struct {
	nodes  map[string]*node
	tables map[string][]string
}

func newDBCache() *dbCache {
	n := make(map[string]*node)
	t := make(map[string][]string)
	return &dbCache{nodes: n, tables: t}
}

// LookupTable provides the name of a table in the db, given the IP of a machine,
// and a time or an error if it doesn't exist.
func (dc *dbCache) LookupTable(nodeIP net.IP, t time.Time) (string, error) {
	ipStr := nodeIP.String()
	node, ok := dc.nodes[ipStr]
	if !ok {
		return "", errNoNode
	}

	tName := genTableName(node.nodeName, t, node.nodeDuration)
	nodeTables := dc.tables[node.nodeName]
	for _, name := range nodeTables {
		if name == tName {
			return name, nil
		}
	}

	return "", errNoTable
}

// LookupNode returns the node information given an IP or an error if it doesn't exist.
func (dc *dbCache) LookupNode(nodeIP net.IP) (*node, error) {
	ipStr := nodeIP.String()
	n, ok := dc.nodes[ipStr]
	if !ok {
		return &node{}, errNoNode
	}
	return n, nil
}

func (dc *dbCache) addTable(nodeIP net.IP, t time.Time) {
	n, err := dc.LookupNode(nodeIP)
	if err != nil {
		dblogger.Errorf("error in addTable:%s. Can't add a table for a node isn't known.", err)
		return
	}
	tName := genTableName(n.nodeName, t, n.nodeDuration)
	nodeTables := dc.tables[n.nodeName]
	nodeTables = append(nodeTables, tName)
	dc.tables[n.nodeName] = nodeTables
}

func (dc *dbCache) addNode(n *node) {
	dc.nodes[n.nodeIP] = n
}

type nestedTableCache struct {
	par    tableCache
	nodes  map[string]*node
	tables map[string][]string
}

func newNestedTableCache(par tableCache) *nestedTableCache {
	n := make(map[string]*node)
	t := make(map[string][]string)
	return &nestedTableCache{par: par, nodes: n, tables: t}
}

func (ntc *nestedTableCache) LookupTable(nodeIP net.IP, t time.Time) (string, error) {
	var node *node
	var err error
	node, ok := ntc.nodes[nodeIP.String()]

	if !ok {
		node, err = ntc.par.LookupNode(nodeIP)
		if err != nil {
			return "", err
		}
		ntc.addNode(node)
	}

	tName := genTableName(node.nodeName, t, node.nodeDuration)
	nodeTables := ntc.tables[node.nodeName]
	ok = false
	for _, name := range nodeTables {
		if name == tName {
			ok = true
			break
		}
	}

	if ok {
		return tName, nil
	}

	tName, err = ntc.par.LookupTable(nodeIP, t)
	if err != nil {
		return "", err
	}
	ntc.addTable(node.nodeName, tName)
	return tName, nil
}

func (ntc *nestedTableCache) LookupNode(nodeIP net.IP) (*node, error) {
	n, ok := ntc.nodes[nodeIP.String()]
	if !ok {
		return &node{}, fmt.Errorf("No such node")
	}
	return n, nil
}

func (ntc *nestedTableCache) addTable(nodeName, table string) {
	nodeTables := ntc.tables[nodeName]
	nodeTables = append(nodeTables, table)
	ntc.tables[nodeName] = nodeTables
}

func (ntc *nestedTableCache) addNode(n *node) {
	ntc.nodes[n.nodeIP] = n
}

// genTableName takes a name of a collector, a time and a duration, and
// it creates a tablename for the relations that will hold the relevant captures.
// it uses underscores as a field separator because they don't have any effect in SQL.
func genTableName(colName string, date time.Time, ddm int) string {
	dur := time.Duration(ddm) * time.Minute
	trunctime := date.Truncate(dur).UTC()
	return fmt.Sprintf("%s_%s", colName, trunctime.Format("2006_01_02_15_04_05"))
}

// ReadFilter is an object passed to ReadStream's so they know what to return
type ReadFilter struct {
	collector string
	start     time.Time
	end       time.Time
}

// Capture represent a BGPCapture as it exists in the database
type Capture struct {
	fromTable string //mostly debug
	id        string //the capture_id that together with the table make it unique
	origin    int    //origin as
	protomsg  []byte //the protobuf blob
}
