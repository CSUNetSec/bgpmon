package util

import (
	"database/sql"
	_ "github.com/lib/pq"
	"testing"
	"time"
)

type TestExecutor struct {
	t        *testing.T
	lastStmt string
	lastVals []interface{}
}

func (te *TestExecutor) checkLast(query string, args ...interface{}) bool {
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
	buf := NewInsertBuffer(2, testEx, base, false, 3)
	buf.Add(1, 2, 3)
	buf.Add(4, 5, 6)
	buf.Add(8, 10, 12)
	pass := testEx.checkLast(base+" (?,?,?),(?,?,?);", 1, 2, 3, 4, 5, 6)
	if !pass {
		t.Logf("Expected: %s %v", base+" (?,?,?),(?,?,?);", []int{1, 2, 3, 4, 5, 6})
		t.Fatalf("Received: %s %v", testEx.lastStmt, testEx.lastVals)
	}
	err := buf.Flush()
	if err != nil {
		t.Fatal(err)
	}

	pass = testEx.checkLast(base+" (?,?,?);", 8, 10, 12)
	if !pass {
		t.Logf("Expected: %s %v", base+" (?,?,?);", []int{8, 10, 12})
		t.Fatalf("Received: %s %v", testEx.lastStmt, testEx.lastVals)
	}
}

func TestTimedBuffer(t *testing.T) {
	// Since this test has a time delay
	if testing.Short() {
		t.SkipNow()
	}

	base := "INSERT INTO timed VALUES"
	testEx := &TestExecutor{t: t}
	buf := NewInsertBuffer(2, testEx, base, false, 3)
	tbuf := NewTimedBuffer(buf, 3*time.Second)

	tbuf.Add(11, 13, 15)
	tbuf.Add(17, 19, 21) // Should get flushed here
	tbuf.Add(23, 25, 27) // These values should stay in the buffer until at least 3 seconds have passed
	time.Sleep(1 * time.Second)
	pass := testEx.checkLast(base+" (?,?,?),(?,?,?);", 11, 13, 15, 17, 19, 21)
	if !pass {
		t.Logf("Expected: %s %v", base+" (?,?,?),(?,?,?);", []int{11, 13, 15, 17, 19, 21})
		t.Fatalf("Received: %s %v", testEx.lastStmt, testEx.lastVals)
	}

	time.Sleep(3 * time.Second)
	pass = testEx.checkLast(base+" (?,?,?);", 23, 25, 27)
	if !pass {
		t.Logf("Expected: %s %v", base+" (?,?,?);", []int{23, 25, 27})
		t.Fatalf("Received: %s %v", testEx.lastStmt, testEx.lastVals)
	}

	tbuf.Stop()
}

// This test has no fail condition, but it's success can be observed
// by selecting on the test table
func TestBufferOnDb(t *testing.T) {
	db, err := getDbConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	baseStmt := "INSERT INTO test VALUES"
	buf := NewInsertBuffer(2, db, baseStmt, true, 3)
	err = buf.Add(21, 22, 23)
	if err != nil {
		t.Fatal(err)
	}
	err = buf.Add(34, 35, 36)
	if err != nil {
		t.Fatal(err)
	}
	err = buf.Add(47, 48, 49)
	if err != nil {
		t.Fatal(err)
	}
	err = buf.Flush()
	if err != nil {
		t.Fatalf("Second error: %v", err)
	}
}

func getDbConnection() (*sql.DB, error) {
	pgconstr := "user=bgpmon password=bgpmon dbname=bgpmon host=localhost sslmode=disable"
	return sql.Open("postgres", pgconstr)

}
