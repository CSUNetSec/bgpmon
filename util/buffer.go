package util

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// SQLBuffer is an interface used to describe anything that can buffer SQL values to be flushed later.
type SQLBuffer interface {
	SQLExecutor
	// Add should add all the provided arguments to the buffer. This can error
	// if improper arguments are added, or in the case of an InsertBuffer, the
	// capacity was reached and the Flush failed.
	Add(arg ...interface{}) error

	// Flush will execute the statement provided to the buffer with
	// the provided values. If it does not return an error, it should
	// leave the buffer in a cleared state.
	Flush() error

	// Clear removes all of the previously added values without flushing
	// them.
	Clear()
}

// InsertBuffer Represents the VALUES() array for an insert statement.
// Helps to optimize the amount of inserted values on a
// single query.
type InsertBuffer struct {
	ex         SQLExecutor     // Executor to flush to
	stmt       string          // Base INSERT statement
	stmtBldr   strings.Builder // Efficiently builds the added statement
	max        int             // The number of adds to flush after
	ct         int             // Current number of entries
	batchSize  int             // Number of arguments expected of an add
	values     []interface{}   // Buffered values
	usePosArgs bool            // Use $1 style args in the statement instead of ?
	first      bool
}

// NewInsertBuffer returns a SQLBuffer which buffers values for an insert statement.
// stmt is the original SQL statement which will be appended with VALUES() clauses. ex
// is the SQLExecutor that the query will be run on when the buffer is full. max is
// the number of VALUES clauses that can be added to this buffer. Each VALUES() clause must
// contain exactly batchSize values. If usePositional is true, the resulting insert statement
// will use this format: ($1, $2, $3). If it is false, it will use: (?,?,?).
func NewInsertBuffer(ex SQLExecutor, max int, usePositional bool) *InsertBuffer {
	return &InsertBuffer{
		ex:         ex,
		max:        max,
		usePosArgs: usePositional,
		first:      true,
	}
}

// Add Satisfies the SQLBuffer interface. This can return an error if the buffer
// is full but fails to flush, or if the length of arg is not equal to the batch
// size.
func (ib *InsertBuffer) Add(arg ...interface{}) error {
	if len(arg) != ib.batchSize {
		return fmt.Errorf("incorrect number of arguments. Expected: %d, Got %d", ib.batchSize, len(arg))
	}

	ib.stmtBldr.WriteString("(")
	for i := range arg {
		idx := ",?"
		if ib.usePosArgs {
			// There is no $0
			idx = fmt.Sprintf(",$%d", (ib.ct*ib.batchSize)+i+1)
		}

		// If this is the first value, ignore the comma
		if i == 0 {
			ib.stmtBldr.WriteString(idx[1:])
		} else {
			ib.stmtBldr.WriteString(idx)
		}
	}
	ib.stmtBldr.WriteString("),")

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

	addedStmt := ib.stmtBldr.String()
	if addedStmt[len(addedStmt)-1] != ',' {
		return fmt.Errorf("improperly terminated statement: %s", addedStmt)
	}

	addedStmt = addedStmt[:len(addedStmt)-1] + ";"

	combinedStmt := fmt.Sprintf("%s %s", ib.stmt, addedStmt)

	_, err := ib.ex.Exec(combinedStmt, ib.values...)
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
	ib.stmtBldr.Reset()
}

// Exec allows the insert buffer to adhere to the SQLExecutor interface.
func (ib *InsertBuffer) Exec(query string, arg ...interface{}) (sql.Result, error) {
	if ib.first {
		ib.first = false
		ib.stmt = query
		ib.batchSize = len(arg)
	}

	if ib.stmt != query {
		return nil, fmt.Errorf("InsertBuffer can't be used to run multiple queries")
	}

	if len(arg) != ib.batchSize {
		return nil, fmt.Errorf("incorrect number of arguments. Expected: %d, Got %d", ib.batchSize, len(arg))
	}

	ib.stmtBldr.WriteString("(")
	for i := range arg {
		idx := ",?"
		if ib.usePosArgs {
			// There is no $0
			idx = fmt.Sprintf(",$%d", (ib.ct*ib.batchSize)+i+1)
		}

		// If this is the first value, ignore the comma
		if i == 0 {
			ib.stmtBldr.WriteString(idx[1:])
		} else {
			ib.stmtBldr.WriteString(idx)
		}
	}
	ib.stmtBldr.WriteString("),")

	ib.values = append(ib.values, arg...)
	ib.ct++

	if ib.ct >= ib.max {
		return nil, ib.Flush()
	}

	return nil, nil
}

// Query allows the insert buffer to adhere to the SQLExecutor interface.
func (ib *InsertBuffer) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return ib.ex.Query(query, args...)
}

// QueryRow allows the insert buffer to adhere to the SQLExecutor interface.
func (ib *InsertBuffer) QueryRow(query string, args ...interface{}) *sql.Row {
	return ib.ex.QueryRow(query, args...)
}

// Commit allows the insert buffer to adhere to the AtomicSQLExecutor interface.
func (ib *InsertBuffer) Commit() error {
	return ib.Flush()
}

// Rollback allows the insert buffer to adhere to the AtomicSQLExecutor interface.
func (ib *InsertBuffer) Rollback() error {
	ib.Clear()
	return nil
}

// TimedBuffer is a SQLBuffer that wraps another SQLBuffer, flushing it after a
// certain amount of time.
type TimedBuffer struct {
	SQLBuffer
	tick     *time.Timer
	duration time.Duration
}

// NewTimedBuffer returns a TimedBuffer that expires after duration d.
func NewTimedBuffer(parent SQLBuffer, d time.Duration) *TimedBuffer {
	t := &TimedBuffer{SQLBuffer: parent, duration: d}
	t.tick = time.AfterFunc(d, func() {
		// This returns an error, but has no way of passing it upward
		// or logging it.
		t.Flush()
	})
	return t
}

// Add Satisfies the SQLBuffer interface. For a TimedBuffer, the args are
// added to the underlying buffer and the timer is reset.
func (t *TimedBuffer) Add(args ...interface{}) error {
	if err := t.SQLBuffer.Add(args...); err != nil {
		return err
	}
	t.tick.Reset(t.duration)
	return nil
}

// Stop is unique to the TimedBuffer, it prevents the buffer from
// expiring after the provided duration.
func (t *TimedBuffer) Stop() {
	t.tick.Stop()
}
