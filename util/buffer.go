package util

import (
	"fmt"
	"strings"
	"time"
)

// SQLBuffer is an interface used to describe anything that can buffer sql values to be flushed later
type SQLBuffer interface {
	Add(arg ...interface{}) error
	Flush() error
	Clear()
}

// InsertBuffer Represents the VALUES() array for an insert statement
// Helps to optimize the amount of inserted values on a
// single query
type InsertBuffer struct {
	ex         SQLExecutor     // Executor to flush to (with the ability to set the internal error)
	stmt       string          // base stmt
	addedStmt  string          // generated statement
	stmtbld    strings.Builder //to efficiently build the string
	max        int             // Depending on the database, might be limited
	ct         int             // Current number of entries
	batchSize  int             // Number of arguments expected of an add
	values     []interface{}   // Buffered values
	usePosArgs bool            // Use $1 style args in the statement instead of ?
}

// NewInsertBuffer returns a SQLBuffer which buffers values for an insert statement.
// stmt is the original SQL statement which will be appended with VALUES() clauses. ex
// is the SQLExecutor that the query will be run on when the buffer is full. max is
// the number of VALUES clauses that can be added to this buffer. Each VALUES() clause must
// contain exactly batchSize values. If usePositional is true, the resulting insert statement
// will use this format: ($1, $2, $3). If it is false, it will use: (?,?,?)
func NewInsertBuffer(ex SQLExecutor, stmt string, max int, batchSize int, usePositional bool) SQLBuffer {
	return &InsertBuffer{max: max, ex: ex, stmt: stmt, addedStmt: "", ct: 0, usePosArgs: usePositional, batchSize: batchSize}
}

// Add Satisfies the SQLBuffer interface. This can return an error if the buffer
// is full but fails to flush, or if the length of arg is not equal to the batch
// size
func (ib *InsertBuffer) Add(arg ...interface{}) error {
	if len(arg) != ib.batchSize {
		return fmt.Errorf("Incorrect number of arguments. Expected: %d, Got %d", ib.batchSize, len(arg))
	}

	ib.stmtbld.WriteString("(")
	for i := range arg {
		idx := ",?"
		if ib.usePosArgs {
			// There is no $0
			idx = fmt.Sprintf(",$%d", (ib.ct*ib.batchSize)+i+1)
		}

		if i == 0 {
			ib.stmtbld.WriteString(idx[1:])
		} else {
			ib.stmtbld.WriteString(idx)
		}
	}
	ib.stmtbld.WriteString("),")

	ib.values = append(ib.values, arg...)
	ib.ct++

	if ib.ct >= ib.max {
		return ib.Flush()
	}
	return nil
}

// Flush Satisfies the SQLBuffer interface
func (ib *InsertBuffer) Flush() error {
	var convStmt string
	if ib.ct == 0 {
		return nil
	}
	ib.addedStmt = ib.stmtbld.String()

	if ib.addedStmt[len(ib.addedStmt)-1] != ',' {
		return fmt.Errorf("Improperly formatted statement: %s", ib.addedStmt)
	}

	ib.addedStmt = ib.addedStmt[:len(ib.addedStmt)-1]
	ib.addedStmt += ";"

	convStmt = fmt.Sprintf("%s %s", ib.stmt, ib.addedStmt)

	_, err := ib.ex.Exec(convStmt, ib.values...)
	if err != nil {
		return err
	}
	ib.Clear()
	return nil
}

// Clear Satisfies the SQLBuffer interface
func (ib *InsertBuffer) Clear() {
	ib.values = ib.values[:0]
	ib.ct = 0
	ib.addedStmt = ""
	ib.stmtbld.Reset()
}

// TimedBuffer is a SQLBuffer that wraps another SQLBuffer, flushing it after a certain amount of time
type TimedBuffer struct {
	SQLBuffer
	lastUpdate time.Time
	tick       *time.Ticker
	duration   time.Duration
	cancel     chan bool
}

// NewTimedBuffer returns a TimedBuffer that expires after duration d
func NewTimedBuffer(parent SQLBuffer, d time.Duration) *TimedBuffer {
	cancel := make(chan bool)
	t := &TimedBuffer{SQLBuffer: parent, lastUpdate: time.Now().UTC(), tick: time.NewTicker(d), duration: d, cancel: cancel}
	go t.wait()
	return t
}

// Add Satisfies the SQLBuffer interface
func (t *TimedBuffer) Add(args ...interface{}) error {
	err := t.SQLBuffer.Add(args...)
	if err != nil {
		return err
	}
	t.lastUpdate = time.Now().UTC()
	return nil
}

// Stop is unique to the TimedBuffer, it stops the timer and closes the waiting goroutine
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
