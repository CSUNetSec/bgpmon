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

type dbOp int

// This block holds the supported operations on the databases.
const (
	connectNoSSLOp = dbOp(iota)
	connectSSLOp
	checkSchemaOp
	selectNodeOp
	insertNodeOp
	insertMainTableOp
	makeMainTableOp
	selectTableOp
	makeNodeTableOp
	makeCaptureTableOp
	insertCaptureTableOp
	getCaptureTablesOp
	getCaptureBinaryOp
	getPrefixOp
)

// dbOps associates every generic database operation with an array that holds the correct SQL statements
// for each type of database. The index in the value array is controlled by the database const.
var dbOps = map[dbOp][]string{
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
		`SELECT d.dbname, d.collector, d.dateFrom, d.dateTo, n.tableDumpDurationMinutes FROM %s d,%s n
                WHERE d.dateFrom <= $1 AND d.dateTo > $1 AND n.ip = $2 AND n.name = d.collector;`,
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
		// postgres
		`SELECT dbname FROM %s WHERE collector='%s' AND dateFrom>='%s' AND dateTo<'%s' ;`,
	},
	getCaptureBinaryOp: {
		// postgres
		`SELECT update_id, origin_as, protomsg FROM %s;`,
	},
	getPrefixOp: {
		// postgres
		`SELECT unnest(adv_prefixes) FROM %s`,
	},
}

// dbLogger is the logger for the database subsystem.
var (
	dbLogger = util.NewLogger("system", "db")
)

// DBer is an interface that returns a reference to the underlying *sql.DB
type DBer interface {
	DB() *sql.DB
}

// TimeoutDBer composes DBer and GetTimeouter to return timeout duration for dbOps
type TimeoutDBer interface {
	DBer
	util.GetTimeouter
}

type queryProvider interface {
	getQuery(dbOp) string
}

type mapProvider struct {
	dbType int
}

func newPostgressQueryProvider() *mapProvider {
	return &mapProvider{
		dbType: postgres,
	}
}

// Returns a query specified by op for the dbType that it was instanciated with.
// Panics on failure.
func (m *mapProvider) getQuery(op dbOp) string {

	queries, exists := dbOps[op]
	if !exists {
		dbLogger.Fatalf("nx db op: %d requested.", op)
	}
	if len(queries)-1 < m.dbType {
		dbLogger.Fatalf("dbop:%s for this db type not populated", op)
	}
	return queries[m.dbType]
}

// SessionExecutor wraps an util.SQLExecutor with queryProvider
type SessionExecutor struct {
	util.SQLExecutor
	queryProvider
}

func newSessionExecutor(ex util.SQLExecutor, oper queryProvider) SessionExecutor {
	return SessionExecutor{SQLExecutor: ex, queryProvider: oper}
}

func (s SessionExecutor) getExecutor() util.SQLExecutor {
	return s.SQLExecutor
}

func (s SessionExecutor) getQueryProvider() queryProvider {
	return s.queryProvider
}

// ctxExecutor is a AtomicSQLExecutor that can timeout via a context. It works
// internally with transaction, so it must be rolled back or committed.
type ctxExecutor struct {
	tx  *sql.Tx
	cf  context.CancelFunc
	ctx context.Context
}

// newCtxExecutor creates a new ctxExecutor and opens the transaction. Once
// this is called, the DB timeout is active. If this object is still in use
// when the timeout comes, the transaction will be rolled back and all further
// calls on this object will return an error.
func newCtxExecutor(tdb TimeoutDBer) (*ctxExecutor, error) {
	ctx, cf := context.WithTimeout(context.Background(), tdb.GetTimeout())

	db := tdb.DB()
	tx, err := db.BeginTx(ctx, nil)

	if err != nil {
		return nil, err
	}

	return &ctxExecutor{
		tx:  tx,
		cf:  cf,
		ctx: ctx,
	}, nil
}

// Exec makes the CtxExecutor conform to the the sql.DB semantics.
func (c *ctxExecutor) Exec(query string, args ...interface{}) (sql.Result, error) {
	return c.tx.ExecContext(c.ctx, query, args...)
}

// Query makes the CtxExecutor conform to the the sql.DB semantics.
func (c *ctxExecutor) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return c.tx.QueryContext(c.ctx, query, args...)
}

// QueryRow makes the CtxExecutor conform to the the sql.DB semantics.
func (c *ctxExecutor) QueryRow(query string, args ...interface{}) *sql.Row {
	return c.tx.QueryRowContext(c.ctx, query, args...)
}

// Commit makes the CtxExecutor conform the sq.Tx semantics.
func (c *ctxExecutor) Commit() error {
	defer c.cf()
	return c.tx.Commit()
}

// Rollback makes the CtxExecutor conform the sq.Tx semantics.
func (c *ctxExecutor) Rollback() error {
	defer c.cf()
	return c.tx.Rollback()
}

// node is a representation of a machine that is used as a BGP vantage point. It can be either
// a collector or a peer. If it is a collector, it is used to generate table names for captures
// seen by that collector. It can also be geolocated. Nodes can be discovered from stored messages
// or provided from a configuration file.
type node struct {
	name        string
	ip          string
	isCollector bool
	duration    int
	description string
	coords      string
	address     string
}

// newNode creates an empty node.
func newNode() *node {
	return &node{}
}

// nodeConfigFromNode creates a node configuration from a node.
func (n *node) nodeConfigFromNode() config.NodeConfig {
	return config.NodeConfig{
		Name:                n.name,
		IP:                  n.ip,
		IsCollector:         n.isCollector,
		DumpDurationMinutes: n.duration,
		Description:         n.description,
		Coords:              n.coords,
		Location:            n.address,
	}
}

// tableCache provides functions to lookup caches for existing table
// names and nodes.
type tableCache interface {
	LookupTable(net.IP, time.Time) (string, error)
	LookupNode(net.IP) (*node, error)
}

// dbCache implements the tableCache interface. This is meant to be a top level cache.
// If it doesn't contain an entry, it just returns an error.
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

	tName := genTableName(node.name, t, node.duration)
	nodeTables := dc.tables[node.name]
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
		return nil, errNoNode
	}
	return n, nil
}

func (dc *dbCache) addTable(nodeIP net.IP, t time.Time) {
	n, err := dc.LookupNode(nodeIP)
	if err != nil {
		dbLogger.Errorf("error in addTable:%s. Can't add a table for a node isn't known.", err)
		return
	}
	tName := genTableName(n.name, t, n.duration)
	nodeTables := dc.tables[n.name]
	nodeTables = append(nodeTables, tName)
	dc.tables[n.name] = nodeTables
}

func (dc *dbCache) addNode(n *node) {
	dc.nodes[n.ip] = n
}

// nestedTableCache is a second level cache. If it doesn't find an entry, it checks the
// provided first-level cache. If it finds the entry in its parent cache, it will update
// its own data.
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

	tName := genTableName(node.name, t, node.duration)
	nodeTables := ntc.tables[node.name]
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
	ntc.addTable(node.name, tName)
	return tName, nil
}

func (ntc *nestedTableCache) LookupNode(nodeIP net.IP) (*node, error) {
	n, ok := ntc.nodes[nodeIP.String()]
	if !ok {
		return nil, errNoNode
	}
	return n, nil
}

func (ntc *nestedTableCache) addTable(nodeName, table string) {
	nodeTables := ntc.tables[nodeName]
	nodeTables = append(nodeTables, table)
	ntc.tables[nodeName] = nodeTables
}

func (ntc *nestedTableCache) addNode(n *node) {
	ntc.nodes[n.ip] = n
}

// genTableName takes a name of a collector, a time and a duration, and
// creates a tablename for the relations that will hold the relevant captures.
// It uses underscores as a field separator because they don't have any effect in SQL.
func genTableName(colName string, date time.Time, durMins int) string {
	dur := time.Duration(durMins) * time.Minute
	truncTime := date.Truncate(dur).UTC()
	return fmt.Sprintf("%s_%s", colName, truncTime.Format("2006_01_02_15_04_05"))
}

// ReadFilter is an object passed to ReadStream's so they know what to return
type ReadFilter struct {
	collector string
	start     time.Time
	end       time.Time
}

// NewReadFilter constructs a ReadFilter with the specified collector and time span
func NewReadFilter(collector string, s, e time.Time) ReadFilter {
	return ReadFilter{collector: collector, start: s, end: e}
}

// GetWhereClause returns a where clause to describe the filter
func (rf ReadFilter) GetWhereClause() string {
	return ""
}

// Capture represent a BGPCapture as it exists in the database
type Capture struct {
	fromTable string // mostly debug
	id        string // the capture_id that together with the table makes it unique
	origin    int    // origin as
	protoMsg  []byte // the protobuf blob
}
