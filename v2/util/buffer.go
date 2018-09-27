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
	max       int // Depending on the database, might be limited
	ct        int
	ex        SqlExecutor
	stmt      string
	addedStmt string
	values    []interface{}
}

func NewInsertBuffer(max int, ex SqlExecutor, stmt string) *InsertBuffer {
	return &InsertBuffer{max: max, ex: ex, stmt: stmt, addedStmt: "", ct: 0}
}

func (ib *InsertBuffer) Add(arg ...interface{}) error {
	ib.ct++
	if ib.ct > ib.max {
		ib.ct = 0
		if err := ib.Flush(); err != nil {
			return err
		}
		ib.Clear()
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
	return nil
}

func (ib *InsertBuffer) Flush() error {
	ib.addedStmt = ib.addedStmt[:len(ib.addedStmt)-1]
	ib.addedStmt += ";"

	_, err := ib.ex.Exec(fmt.Sprintf("%s %s", ib.stmt, ib.addedStmt), ib.values...)
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
	t := &TimedBuffer{SqlBuffer: parent, lastUpdate: time.Now(), tick: time.NewTicker(d), duration: d, cancel: cancel}
	go t.wait()
	return t
}

func (t *TimedBuffer) Add(args ...interface{}) error {
	err := t.SqlBuffer.Add(args...)
	if err != nil {
		return err
	}
	t.lastUpdate = time.Now()
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
			if time.Now().Sub(t.lastUpdate) > t.duration {
				t.Flush()
			}
		case _, ok := <-t.cancel:
			if !ok {
				return
			}
		}
	}
}
