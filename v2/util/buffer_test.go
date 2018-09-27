package util

import (
	"database/sql"
	"testing"
	"time"
)

type TestExecutor struct {
	t        *testing.T
	lastStmt string
	lastVals []interface{}
}

func (te *TestExecutor) CheckLast(query string, args ...interface{}) bool {
	if len(args) != len(te.lastVals) {
		return false
	}

	if query != te.lastStmt {
		return false
	}

	for i, _ := range args {
		if args[i] != te.lastVals[i] {
			return false
		}
	}
	return true
}

func (te *TestExecutor) Exec(query string, args ...interface{}) (sql.Result, error) {
	te.t.Logf("Test Exec called with: %s %v", query, args)
	te.lastStmt = query
	te.lastVals = make([]interface{}, len(args))
	copy(te.lastVals, args)
	return nil, nil
}

func (t TestExecutor) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return nil, nil
}

func (t TestExecutor) QueryRow(query string, args ...interface{}) *sql.Row {
	return nil
}

func TestInsertBuffer(t *testing.T) {
	base := "INSERT INTO testTable VALUES"
	testEx := &TestExecutor{t: t}
	buf := NewInsertBuffer(2, testEx, base)
	buf.Add(1, 2, 3)
	buf.Add(4, 5, 6)
	buf.Add(8, 10, 12)
	pass := testEx.CheckLast(base+" "+"(?,?,?),(?,?,?);", 1, 2, 3, 4, 5, 6)
	if !pass {
		t.Fail()
	}
	buf.Flush()
	pass = testEx.CheckLast(base+" "+"(?,?,?);", 8, 10, 12)
	if !pass {
		t.Fail()
	}
}

func TestTimedBuffer(t *testing.T) {
	base := "INSERT INTO timed VALUES"
	testEx := &TestExecutor{t: t}
	buf := NewInsertBuffer(1, testEx, base)
	tbuf := NewTimedBuffer(buf, 3*time.Second)
	tbuf.Add(11, 13, 15)
	tbuf.Add(17, 19, 21)
	time.Sleep(1 * time.Second)
	pass := testEx.CheckLast(base+" (?,?,?);", 11, 13, 15)
	if !pass {
		t.FailNow()
	}

	time.Sleep(3 * time.Second)
	pass = testEx.CheckLast(base+" (?,?,?);", 17, 19, 21)
	if !pass {
		t.FailNow()
	}
}
