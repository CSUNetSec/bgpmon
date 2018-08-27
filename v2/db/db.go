package db

import (
	"context"
	"database/sql"
	"github.com/sirupsen/logrus"
	"time"
)

var (
	dblogger = logrus.WithField("system", "db")
)

type Dber interface {
	Db() *sql.DB
}

// sqlCtxExecutor is an interface needed for basic queries.
// It is implemented partly by both sql.DB and sql.Txn but we will
// implemented on our wrapper ctxTx. ctxTx wraps these functions with a context
// and deeper their xxxContext variant is called.
type sqlCtxExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
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
//it will implement the sqlCtxExectutor interface and choose
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
