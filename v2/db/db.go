package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/CSUNetSec/bgpmon/v2/config"
	"github.com/CSUNetSec/bgpmon/v2/util"
	"time"
)

const (
	postgres = iota
)

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
)

var dbops = map[string][]string{
	connectNoSSLOp: []string{
		//postgres
		`user=%s password=%s dbname=%s host=%s sslmode=disable`,
	},
	connectSSLOp: []string{
		//postgres
		`user=%s password=%s dbname=%s host=%s`,
	},
	checkSchemaOp: []string{
		//postgres
		`SELECT EXISTS (
		   SELECT *
		   FROM   information_schema.tables
		   WHERE  table_name = $1
		 );`,
	},
	selectNodeOp: []string{
		//postgres
		`SELECT name, ip, isCollector, tableDumpDurationMinutes, description, coords, address FROM %s;`,
	},
	insertNodeOp: []string{
		//postgres
		`INSERT INTO %s (name, ip, isCollector, tableDumpDurationMinutes, description, coords, address) 
		   VALUES ($1, $2, $3, $4, $5, $6, $7)
		   ON CONFLICT (ip) DO UPDATE SET name=EXCLUDED.name, isCollector=EXCLUDED.isCollector, 
		     tableDumpDurationMinutes=EXCLUDED.tableDumpDurationMinutes,
		     description=EXCLUDED.description, coords=EXCLUDED.coords, address=EXCLUDED.address;`,
	},
	makeMainTableOp: []string{
		//postgres
		`CREATE TABLE IF NOT EXISTS %s (
		   dbname varchar PRIMARY KEY,
	           collector varchar NOT NULL,
	           dateFrom timestamp NOT NULL,
	           dateTo timestamp NOT NULL
                 );`,
	},
	insertMainTableOp: []string{
		//postgres
		`INSERT INTO %s (dbname, collector, dateFrom, dateTo) VALUES ($1, $2, $3, $4);`,
	},
	makeCaptureTableOp: []string{
		//postgres
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
	insertCaptureTableOp: []string{
		//postgres
		`INSERT INTO %s (timestamp, collector_ip, peer_ip, as_path, next_hop, origin_as, adv_prefixes, wdr_prefixes, protomsg) VALUES `,
	},
	selectTableOp: []string{
		//postgres
		`SELECT dbname, collector, dateFrom, dateTo, tableDumpDurationMinutes FROM %s,%s 
		 WHERE dateFrom <= $1 AND dateTo > $1 AND ip = $2;`,
	},
	makeNodeTableOp: []string{
		//postgres
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
	getCaptureTablesOp: []string{
		`SELECT dbname FROM %s WHERE collector=$1 AND dateFrom>=$2 AND dateTo<$3;`,
	},
}

var (
	dblogger = util.NewLogger("system", "db")
)

//Dber is an interface that returns a reference to the underlying *sql.DB
type Dber interface {
	Db() *sql.DB
}

//a struct for issuing queries about the existance of a ready collector table
//for a specific time. Typically on the return we will return the starting
//time for that table as a string so that the caller can just concat and create
//the destination table names
type collectorDate struct {
	col    string    //the collector we are querying for
	dat    time.Time //the time we are interested
	datstr string    //the time string returned that will create the table name
}

func newCollectorDate(col string, t time.Time) collectorDate {
	return collectorDate{
		col: col,
		dat: t,
	}
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
		t: postgres,
	}
}

//SessionExecutor wraps an util.SqlErrorExecutor with getdboper
type SessionExecutor interface {
	util.SqlErrorExecutor
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
	err error
}

func (d *dbOperExecutor) SetError(e error) {
	dblogger.Infof("setting the dbOperExecutor error to:%s. It will not have an effect since we are not in a tx.", e)
	d.err = e
}

func newDbSessionExecutor(db *sql.DB, dbo *dbOper) *dbOperExecutor {
	return &dbOperExecutor{
		db,
		dbo,
		nil,
	}
}

//GetNewExecutor creates a new ctxTx for that operation which implements the
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
		err:  nil,
	}, nil
}

//it will set the internal error state of the ctxtx and cause the rest of the functions to become noops.
//if a transaction is involved, it will be rolled back
func (c *ctxTx) SetError(e error) {
	c.err = e
	c.Done()
}

func (c *ctxTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	if c.err != nil {
		dblogger.Infof("Exec called in ctxTx but err has been set:%s", c.err)
		return nil, c.err
	}
	if c.doTx && c.tx != nil {
		return c.tx.ExecContext(c.ctx, query, args...)
	}
	return c.db.ExecContext(c.ctx, query, args...)
}

func (c *ctxTx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if c.err != nil {
		dblogger.Infof("Query called in ctxTx but err has been set:%s", c.err)
		return nil, c.err
	}
	if c.doTx && c.tx != nil {
		return c.tx.QueryContext(c.ctx, query, args...)
	}
	return c.db.QueryContext(c.ctx, query, args...)
}

func (c *ctxTx) QueryRow(query string, args ...interface{}) *sql.Row {
	if c.err != nil {
		dblogger.Infof("QueryRow called in ctxTx but err has been set:%s", c.err)
		return nil
	}
	if c.doTx && c.tx != nil {
		return c.tx.QueryRowContext(c.ctx, query, args...)
	}
	return c.db.QueryRowContext(c.ctx, query, args...)
}

//a wrapper of a sql.Tx that is able to accept multiple
//db ops and run them in the same tx.
//it will implement the SqlErrorExectutor interface and choose
//where to apply the sql function depending on how it was constructed.
//(either apply everything in the transaction and then the last Done()
//will commit, or straight on the DB and the last Done() is a noop.
//the ctxTx structs are created by the specific sessions.
//if there was an error that was set though, and it has a transaction , it
//will be rolled back
type ctxTx struct {
	doTx bool
	tx   *sql.Tx
	db   *sql.DB
	err  error
	cf   context.CancelFunc
	ctx  context.Context
}

//either commits the TX or just releases the context through it's cancelfunc.
func (c *ctxTx) Done() error {
	defer c.cf() //release resources if it's done.
	if c.doTx && c.tx != nil {
		if c.err == nil {
			dblogger.Infof("tx commit successfull")
			return c.tx.Commit()
		}
		dblogger.Infof("rolling back the transaction due to error:%s", c.err)
		return c.tx.Rollback()
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
