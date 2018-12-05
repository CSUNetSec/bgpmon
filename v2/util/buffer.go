package util

import (
	"fmt"
	"time"
)

type SqlBuffer interface {
	Add(arg ...interface{}) error
	Flush() error
	Clear()
}

// Represents the VALUES() array for an insert statement
// Helps to optimize the amount of inserted values on a
// single query
type InsertBuffer struct {
	ex         SqlExecutor   // Executor to flush to
	stmt       string        // base stmt
	addedStmt  string        // generated statement
	max        int           // Depending on the database, might be limited
	ct         int           // Current number of entries
	batchSize  int           // Number of arguments expected of an add
	values     []interface{} // Buffered values
	usePosArgs bool          // Use $1 style args in the statement instead of ?
}

func NewInsertBuffer(ex SqlExecutor, stmt string, max int, batchSize int, usePositional bool) *InsertBuffer {
	return &InsertBuffer{max: max, ex: ex, stmt: stmt, addedStmt: "", ct: 0, usePosArgs: usePositional, batchSize: batchSize}
}

func (ib *InsertBuffer) Add(arg ...interface{}) error {
	if len(arg) != ib.batchSize {
		return fmt.Errorf("Incorrect number of arguments. Expected: %d, Got %d", ib.batchSize, len(arg))
	}

	ib.addedStmt += "("
	for i := range arg {
		if i == 0 {
			ib.addedStmt += "?"
		} else {
			ib.addedStmt += ",?"
		}
	}
	ib.addedStmt += "),"

	ib.values = append(ib.values, arg...)
	ib.ct++

	if ib.ct >= ib.max {
		return ib.Flush()
	}
	return nil
}

func (ib *InsertBuffer) Flush() error {
	if ib.ct == 0 {
		return nil
	}

	if ib.addedStmt[len(ib.addedStmt)-1] != ',' {
		return fmt.Errorf("Improperly formatted statement: %s", ib.addedStmt)
	}

	ib.addedStmt = ib.addedStmt[:len(ib.addedStmt)-1]
	ib.addedStmt += ";"

	convStmt := ""
	if ib.usePosArgs {
		convStmt = convertSqlStmt(fmt.Sprintf("%s %s", ib.stmt, ib.addedStmt))
	} else {
		convStmt = fmt.Sprintf("%s %s", ib.stmt, ib.addedStmt)
	}
	_, err := ib.ex.Exec(convStmt, ib.values...)
	if err != nil {
		return err
	}
	ib.Clear()
	return nil
}

func (ib *InsertBuffer) Clear() {
	ib.values = ib.values[:0]
	ib.ct = 0
	ib.addedStmt = ""
}

type TimedBuffer struct {
	SqlBuffer
	lastUpdate time.Time
	tick       *time.Ticker
	duration   time.Duration
	cancel     chan bool
}

func NewTimedBuffer(parent SqlBuffer, d time.Duration) *TimedBuffer {
	cancel := make(chan bool)
	t := &TimedBuffer{SqlBuffer: parent, lastUpdate: time.Now().UTC(), tick: time.NewTicker(d), duration: d, cancel: cancel}
	go t.wait()
	return t
}

func (t *TimedBuffer) Add(args ...interface{}) error {
	err := t.SqlBuffer.Add(args...)
	if err != nil {
		return err
	}
	t.lastUpdate = time.Now().UTC()
	return nil
}

func (t *TimedBuffer) Stop() {
	t.tick.Stop()
	close(t.cancel)
}

func (t *TimedBuffer) wait() {
	for {
		select {
		case <-t.tick.C:
			if time.Now().UTC().Sub(t.lastUpdate) > t.duration {
				t.Flush()
			}
		case _, ok := <-t.cancel:
			if !ok {
				return
			}
		}
	}
}
