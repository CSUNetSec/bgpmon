package util

import (
	"database/sql"
	"fmt"
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

	for i := range args {
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

func (te TestExecutor) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return nil, nil
}

func (te TestExecutor) QueryRow(query string, args ...interface{}) *sql.Row {
	return nil
}

func (te TestExecutor) SetError(e error) {
	return
}

func TestInsertBuffer(t *testing.T) {
	base := "INSERT INTO testTable VALUES"
	testEx := &TestExecutor{t: t}
	buf := NewInsertBuffer(testEx, base, 2, 3, false)
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

	base := "INSERT INTO timed VALUES"
	testEx := &TestExecutor{t: t}
	buf := NewInsertBuffer(testEx, base, 2, 3, false)
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

type dbWrapper struct {
	SQLExecutor
}

func (d dbWrapper) SetError(e error) {}

var wrapper dbWrapper

// This test has no fail condition, but it's success can be observed
// by selecting on the test table
func TestBufferOnDb(t *testing.T) {
	if testing.Short() {
		t.Skipf("Skipping TestBufferOnDb on short tests")
	}
	dbConn, err := getDbConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer dbConn.Close()
	wrapper = dbWrapper{SQLExecutor: dbConn}

	err = setupTestTable()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("", dbBufferTest)

	err = teardownTestTable()
	if err != nil {
		t.Fatal(err)
	}
}

func setupTestTable() error {
	stmt := "CREATE TABLE IF NOT EXISTS test (a int, b int, c int);"
	_, err := wrapper.Exec(stmt)
	return err
}

func teardownTestTable() error {
	stmt := "DROP TABLE test;"
	_, err := wrapper.Exec(stmt)
	return err
}

func dbBufferTest(t *testing.T) {
	baseStmt := "INSERT INTO test VALUES"
	queryStmt := "SELECT * FROM test;"
	buf := NewInsertBuffer(wrapper, baseStmt, 2, 3, true)

	err := buf.Add(21, 22, 23)
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
		t.Fatal(err)
	}

	rows, err := wrapper.Query(queryStmt)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	sum := 0
	for rows.Next() {
		a, b, c := 0, 0, 0
		rows.Scan(&a, &b, &c)
		sum += a + b + c
	}
	expected := 66 + 105 + 144

	if sum != expected {
		t.Errorf("Expected %d, Got %d", expected, sum)
	}

}

func getDbConnection() (*sql.DB, error) {
	pgconstr := "user=bgpmon password=bgpmon dbname=bgpmon host=localhost sslmode=disable"
	return sql.Open("postgres", pgconstr)
}

func TestBufferBatchSize(t *testing.T) {
	testEx := &TestExecutor{t: t}
	buf := NewInsertBuffer(testEx, "", 2, 3, false)
	err := buf.Add(1, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	err = buf.Add(4, 5, 6)
	if err != nil {
		t.Fatal(err)
	}
	err = buf.Add(7, 8, 9, 10)
	if err == nil {
		t.Fatal(fmt.Errorf("Error expected but not received"))
	}
	err = buf.Add(11, 12)
	if err == nil {
		t.Fatal(fmt.Errorf("Error expected but not received"))
	}
}
