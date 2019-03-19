package util

import (
	"fmt"
	"strings"
	"time"
)

// SQLBuffer is an interface used to describe anything that can buffer SQL values to be flushed later
type SQLBuffer interface {
	Add(arg ...interface{}) error
	Flush() error
	Clear()
}

// InsertBuffer Represents the VALUES() array for an insert statement
// Helps to optimize the amount of inserted values on a
// single query
type InsertBuffer struct {
	ex         SQLExecutor     // Executor to flush to
	stmt       string          // Base INSERT statement
	stmtbld    strings.Builder // Efficiently builds the added statement
	max        int             // The number of adds to flush after
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
	return &InsertBuffer{max: max, ex: ex, stmt: stmt, ct: 0, usePosArgs: usePositional, batchSize: batchSize}
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

		// If this is the first value, ignore the comma
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

// Flush Satisfies the SQLBuffer interface. It will execute the INSERT statement
// on the provided executor.
func (ib *InsertBuffer) Flush() error {
	if ib.ct == 0 {
		return nil
	}

	addedStmt := ib.stmtbld.String()
	if addedStmt[len(addedStmt)-1] != ',' {
		return fmt.Errorf("Improperly formatted statement: %s", addedStmt)
	}

	addedStmt = addedStmt[:len(addedStmt)-1] + ";"

	convStmt := fmt.Sprintf("%s %s", ib.stmt, addedStmt)

	_, err := ib.ex.Exec(convStmt, ib.values...)
	if err != nil {
		return err
	}
	ib.Clear()
	return nil
}

// Clear Satisfies the SQLBuffer interface. It will remove all added values,
// leaving the buffer in a clean state.
func (ib *InsertBuffer) Clear() {
	ib.values = ib.values[:0]
	ib.ct = 0
	ib.stmtbld.Reset()
}

// TimedBuffer is a SQLBuffer that wraps another SQLBuffer, flushing it after a certain amount of time
type TimedBuffer struct {
	SQLBuffer
	tick     *time.Timer
	duration time.Duration
}

// NewTimedBuffer returns a TimedBuffer that expires after duration d
func NewTimedBuffer(parent SQLBuffer, d time.Duration) *TimedBuffer {
	t := &TimedBuffer{SQLBuffer: parent, duration: d}
	ticker := time.AfterFunc(d, func() {
		t.Flush()
	})
	t.tick = ticker
	return t
}

// Add Satisfies the SQLBuffer interface
func (t *TimedBuffer) Add(args ...interface{}) error {
	if err := t.SQLBuffer.Add(args...); err != nil {
		return err
	}
	t.tick.Reset(t.duration)
	return nil
}

// Stop is unique to the TimedBuffer, it stops the timer and closes the waiting goroutine
func (t *TimedBuffer) Stop() {
	t.tick.Stop()
}
