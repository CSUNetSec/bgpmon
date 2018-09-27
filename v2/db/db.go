package db

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/CSUNetSec/bgpmon/v2/config"
	"github.com/CSUNetSec/bgpmon/v2/util"
	"github.com/sirupsen/logrus"
	"time"
)

const (
	POSTGRES = iota
)

const (
	CONNECT_NO_SSL  = "connectNoSSL"
	CONNECT_SSL     = "connectSSL"
	CHECK_SCHEMA    = "checkschema"
	SELECT_NODE     = "selectNodeTmpl"
	INSERT_NODE     = "insertNodeTmpl"
	MAKE_MAIN_TABLE = "makeMainTableTmpl"
	MAKE_NODE_TABLE = "makeNodeTableTmpl"
)

var dbops = map[string][]string{
	CONNECT_NO_SSL: []string{
		//postgress
		`user=%s password=%s dbname=%s host=%s sslmode=disable`,
	},
	CONNECT_SSL: []string{
		//postgress
		`user=%s password=%s dbname=%s host=%s`,
	},
	CHECK_SCHEMA: []string{
		//postgress
		`SELECT EXISTS (
		   SELECT *
		   FROM   information_schema.tables
		   WHERE  table_name = $1
		 );`,
	},
	SELECT_NODE: []string{
		//postgress
		`SELECT name, ip, isCollector, tableDumpDurationMinutes,
		   description, coords, address FROM %s;`,
	},
	INSERT_NODE: []string{
		//postgress
		`INSERT INTO %s (name, ip, isCollector, tableDumpDurationMinutes, description, coords, address) 
		   VALUES ($1, $2, $3, $4, $5, $6, $7)
		   ON CONFLICT (ip) DO UPDATE SET name=EXCLUDED.name, isCollector=EXCLUDED.isCollector, 
		     tableDumpDurationMinutes=EXCLUDED.tableDumpDurationMinutes,
		     description=EXCLUDED.description, coords=EXCLUDED.coords, address=EXCLUDED.address;`,
	},
	MAKE_MAIN_TABLE: []string{
		//postgress
		`CREATE TABLE IF NOT EXISTS %s (
		   dbname varchar PRIMARY KEY,
	           collector varchar,
	           dateFrom timestamp,
	           dateTo timestamp
                 );`,
	},
	MAKE_NODE_TABLE: []string{
		//postgress
		`CREATE TABLE IF NOT EXISTS %s (
		   ip varchar PRIMARY KEY,
		   name varchar, 
		   isCollector boolean,
		   tableDumpDurationMinutes integer,
		   description varchar,
		   coords varchar,
		   address varchar
	         );`,
	},
}

var (
	dblogger = logrus.WithField("system", "db")
)

type Dber interface {
	Db() *sql.DB
}

//a wrapper struct that can contain all the possible arguments to our database calls
type sqlIn struct {
	dbname      string                       //the name of the database we're operating on
	maintable   string                       //the table which references all collector-day tables.
	nodetable   string                       //the table with nodes and their configurations
	knownNodes  map[string]config.NodeConfig //an incoming map of the known nodes
	getNodeName string                       //a node name we want to fetch is config from the db
	getNodeIP   string                       //a node IP we want to fetch is config from the db
}

type sqlOut struct {
	ok         bool
	err        error
	knownNodes map[string]config.NodeConfig //a composition of the incoming and already known nodes
	resultNode *node                        //the result from a getNode call
}

type getdboper interface {
	getdbop(string) string
}

type dbOper struct {
	t int
}

// Gets the specific db op string from the static table declared in db.go
// for the appropriate dbType that was populated when the correct newSession was called.
// Panics on error.
// implementing the getdboper interface in db.go
func (d *dbOper) getdbop(a string) (ret string) {
	if sslice, exists := dbops[a]; !exists {
		panic(fmt.Sprintf("nx db op name:%s requested.", a))
	} else if len(sslice)-1 < d.t {
		panic(fmt.Sprintf("dbop:%s for this db type not populated", a))
	} else {
		ret = sslice[d.t]
	}
	return
}

func newPostgressDbOper() *dbOper {
	return &dbOper{
		t: POSTGRES,
	}
}

type SessionExecutor interface {
	util.SqlExecutor
	getdboper
}

type ctxtxOperExecutor struct {
	*ctxTx
	*dbOper
}

func newCtxTxSessionExecutor(cex *ctxTx, dbo *dbOper) *ctxtxOperExecutor {
	return &ctxtxOperExecutor{
		cex,
		dbo,
	}
}

type dbOperExecutor struct {
	*sql.DB
	*dbOper
}

func newDbSessionExecutor(db *sql.DB, dbo *dbOper) *dbOperExecutor {
	return &dbOperExecutor{
		db,
		dbo,
	}
}

//creates a new ctxTx for that operation which implements the
//sqlExecutor interface. The argument passed instructs it to either
//do it on a transaction if true, or on the normal DB connection if false.
//caller must call Done() that releases resources.
func GetNewExecutor(pc context.Context, s Dber, doTx bool, ctxTimeout time.Duration) (*ctxTx, error) {
	var (
		tx  *sql.Tx
		err error
		db  *sql.DB
	)
	db = s.Db()
	ctx, cf := context.WithTimeout(pc, ctxTimeout)
	if doTx {
		if tx, err = db.BeginTx(ctx, nil); err != nil {
			cf()
			return nil, err
		}
	} else {
		tx = nil
	}
	return &ctxTx{
		doTx: doTx,
		tx:   tx,
		cf:   cf,
		ctx:  ctx,
		db:   db,
	}, nil
}

func (c *ctxTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	if c.doTx && c.tx != nil {
		return c.tx.ExecContext(c.ctx, query, args...)
	}
	return c.db.ExecContext(c.ctx, query, args...)
}

func (c *ctxTx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if c.doTx && c.tx != nil {
		return c.tx.QueryContext(c.ctx, query, args...)
	}
	return c.db.QueryContext(c.ctx, query, args...)
}

func (c *ctxTx) QueryRow(query string, args ...interface{}) *sql.Row {
	if c.doTx && c.tx != nil {
		return c.tx.QueryRowContext(c.ctx, query, args...)
	}
	return c.db.QueryRowContext(c.ctx, query, args...)
}

//a wrapper of a sql.Tx that is able to accept multiple
//db ops and run them in the same tx.
//it will implement the SqlExectutor interface and choose
//where to apply the sql function depending on how it was constructed.
//(either apply everything in the transaction and then the last Done()
//will commit, or straight on the DB and the last Done() is a noop.
//the ctxTx structs are created by the specific sessions.
type ctxTx struct {
	doTx bool
	tx   *sql.Tx
	db   *sql.DB
	cf   context.CancelFunc
	ctx  context.Context
}

//either commits the TX or just releases the context through it's cancelfunc.
func (ptx *ctxTx) Done() error {
	defer ptx.cf() //release resources if it's done.
	if ptx.doTx && ptx.tx != nil {
		return ptx.tx.Commit()
	}
	return nil
}

//This is a representation of a node that is stored in the database using this fields.
//a node can be either a collector or a peer, and in case of being a collector it is used
//to generate the table names that data collected by it are stored. it should be also geolocated.
//known nodes can be supplied by the config file.
type node struct {
	nodeName      string
	nodeIP        string
	nodeCollector bool
	nodeDuration  int
	nodeDescr     string
	nodeCoords    string
	nodeAddress   string
}

//creates an empty node
func newNode() *node {
	return &node{}
}

//creates a nodeconfig from a node
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
