package db

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/CSUNetSec/bgpmon/v2/config"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"time"
)

// sqlCtxExecutor is an interface needed for basic queries.
// It is implemented by both sql.DB and sql.Txn.
type sqlCtxExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type postgresSession struct {
	db     *sql.DB
	dbname string
}

//a wrapper of a sql.Tx that is able to accept multiple
//db ops and run them in the same tx.
type ctxTx struct {
	tx  *sql.Tx
	db  *sql.DB
	cf  context.CancelFunc
	ctx context.Context
}

func (ps *postgresSession) newCtxTx() (*ctxTx, error) {
	var (
		tx  *sql.Tx
		err error
	)
	//XXX configurable timeout?
	ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
	if tx, err = ps.db.Begin(); err != nil {
		return nil, err
	}
	return &ctxTx{
		tx:  tx,
		cf:  cf,
		ctx: ctx,
		db:  ps.db,
	}, nil
}

//a wrapper struct that can contain all the possible arguments to our calls
type sqlIn struct {
	dbname    string //the name of the database we're operating on
	maintable string //the table which references all collector-day tables.
	coltable  string //the table with collectors and their configurations
	peertable string //the table that keeps the peers that are discovered for each collector
}

type sqlOut struct {
	ok  bool
	err error
}

type workFunc func(context.Context, sqlCtxExecutor, sqlIn) sqlOut

//runs something on the transaction if newCtxTx has been invoked with a TX otherwise it runs it
//on the db
func (ptx *ctxTx) Do(a func(context.Context, sqlCtxExecutor, sqlIn) sqlOut, args sqlIn) sqlOut {
	if ptx.tx != nil {
		return a(ptx.ctx, ptx.tx, args)
	}
	return a(ptx.ctx, ptx.db, args)
}

//either commits the TX or just releases the context through it's cancelfunc.
func (ptx *ctxTx) Done() error {
	defer ptx.cf() //release resources if it's done.
	if ptx.tx != nil {
		return ptx.tx.Commit()
	}
	return nil
}

func retCheckSchema(o sqlOut) (bool, error) {
	return o.ok, o.err
}

func checkSchema(ctx context.Context, ex sqlCtxExecutor, args sqlIn) (ret sqlOut) {
	const q = `
SELECT EXISTS (
   SELECT *
   FROM   information_schema.tables
   WHERE  table_name = $1
 );
`
	var (
		res bool
		err error
	)
	tocheck := []string{args.maintable, args.coltable, args.peertable}
	allgood := true
	for _, tname := range tocheck {
		if err = ex.QueryRowContext(ctx, q, tname).Scan(&res); err != nil {
			ret.ok, ret.err = false, errors.Wrap(err, "checkSchema")
			return
		}
		dblogger.Infof("table:%s exists:%v", tname, res)
		allgood = allgood && res
	}
	ret.ok, ret.err = allgood, nil
	return
}

func retMakeSchema(o sqlOut) error {
	return o.err
}

func makeSchema(ctx context.Context, ex sqlCtxExecutor, args sqlIn) (ret sqlOut) {
	const maintableTmpl = `
CREATE TABLE IF NOT EXISTS %s (
	dbname varchar PRIMARY KEY,
	collector varchar,
	dateFrom timestamp,
	dateTo timestamp
); 
`
	const coltableTmpl = `
CREATE TABLE IF NOT EXISTS %s (
	colname varchar PRIMARY KEY,
	colip inet,
	duration interval,
	description varchar,
	coords point,
	address varchar
); 
`
	const peertableTmpl = `
CREATE TABLE IF NOT EXISTS %s (
	peername varchar PRIMARY KEY,
	collector varchar references %s(colname),
	peerip inet,
	description varchar,
 	coords point,
	address varchar
);
`
	var (
		err error
	)
	if _, err = ex.ExecContext(ctx, fmt.Sprintf(maintableTmpl, args.maintable)); err != nil {
		ret.err = errors.Wrap(err, "makeSchema maintable")
		return
	}
	dblogger.Infof("created table:%s", args.maintable)

	if _, err = ex.ExecContext(ctx, fmt.Sprintf(coltableTmpl, args.coltable)); err != nil {
		ret.err = errors.Wrap(err, "makeSchema coltable")
		return
	}
	dblogger.Infof("created table:%s", args.coltable)

	if _, err = ex.ExecContext(ctx, fmt.Sprintf(peertableTmpl, args.peertable, args.coltable)); err != nil {
		ret.err = errors.Wrap(err, "makeSchema peertable")
		return
	}
	dblogger.Infof("created table:%s", args.peertable)
	return
}

func (ps *postgresSession) Write(wr *pb.WriteRequest) error {
	dblogger.Infof("postgres write called with request:%s", wr)
	return nil
}

func (ps *postgresSession) Close() error {
	dblogger.Info("postgres close called")
	if err := ps.db.Close(); err != nil {
		return errors.Wrap(err, "db close")
	}
	return nil
}

func (ps *postgresSession) Schema(sc SchemaCmd) (ret SchemaReply) {
	dblogger.Infof("postgres SchemaCommand called")
	switch sc.cmd {
	case CheckAndInit:
		ret.err = ps.SchemaCheckInit()
	default:
		dblogger.Errorf("Unknown schema command %v", sc)
		ret.err = errors.New("unknown schema command")
	}
	return
}

func newPostgresSession(conf config.SessionConfiger, id string) (Sessioner, error) {
	var (
		db  *sql.DB
		err error
	)
	dblogger.Infof("postgres db session starting")
	u := conf.GetUser()
	p := conf.GetPassword()
	d := conf.GetDatabaseName()
	h := conf.GetHostNames()
	cd := conf.GetCertDir()
	if len(h) == 1 && p != "" && cd == "" && u != "" { //no ssl standard pw
		db, err = sql.Open("postgres", fmt.Sprintf("user=%s password=%s dbname=%s host=%s sslmode=disable", u, p, d, h[0]))
	} else if cd != "" && u != "" { //ssl
		db, err = sql.Open("postgres", fmt.Sprintf("user=%s password=%s dbname=%s host=%s", u, p, d, h[0]))
	} else {
		return nil, errors.New("Postgres sessions require a password and exactly one hostname")
	}
	if err != nil {
		return nil, errors.Wrap(err, "sql open")
	}
	return &postgresSession{db: db, dbname: d}, nil
}

//Checks the state and initializes the appropriate schemas
func (ps *postgresSession) SchemaCheckInit() error {
	//ping to see we can connect to the db
	if err := ps.db.Ping(); err != nil {
		return errors.Wrap(err, "sql ping")
	}
	//check for required main tables on the schema.
	if pctx, err := ps.newCtxTx(); err != nil {
		return errors.Wrap(err, "newCtxTx")
	} else {
		csargs := sqlIn{dbname: "bgpmon", maintable: "dbs", coltable: "collectors", peertable: "peers"}
		if ok, err := retCheckSchema(pctx.Do(checkSchema, csargs)); err != nil {
			return errors.Wrap(err, "retCheckschema")
		} else {
			if !ok { // table does not exist
				dblogger.Infof("creating schema tables")
				if err = retMakeSchema(pctx.Do(makeSchema, csargs)); err != nil {
					return errors.Wrap(err, "retMakeSchema")
				}
				dblogger.Infof("all good. commiting the changes")
				if err = pctx.Done(); err != nil {
					return errors.Wrap(err, "makeschema commit")
				}
			} else {
				dblogger.Infof("all main bgpmon tables exist.")
			}
		}
	}
	return nil
}
