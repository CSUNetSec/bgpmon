package util

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

/*
This file tests the functions and structs defined in buffer.go.
Some of these tests require a live Postgres instance to be running.
The postgres instance has to have a user with these credentials:
	Username: bgpmon
	Password: bgpmon
	Database: bgpmon
*/

// This struct satisfies the SQLExecutor interface, so it can be used
// to validate several functions that require an executor.
type TestExecutor struct {
	t        *testing.T
	lastStmt string
	lastVals []interface{}
}

// checkLast compares the last query done on the executor to the incoming arguments.
// Returns false if they are different.
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

func (te *TestExecutor) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return nil, nil
}

func (te *TestExecutor) QueryRow(query string, args ...interface{}) *sql.Row {
	return nil
}

// TestInsertBuffer tests whether the buffer will flush at the right time.
func TestInsertBuffer(t *testing.T) {
	base := "INSERT INTO testTable VALUES"
	testEx := &TestExecutor{t: t}
	buf := NewInsertBuffer(testEx, 2, false)

	if _, err := buf.Exec(base, 1, 2, 3); err != nil {
		t.Fatalf("Error adding (1,2,3) to buffer: %s", err)
	}
	if _, err := buf.Exec(base, 4, 5, 6); err != nil {
		t.Fatalf("Error adding (4,5,6) to buffer: %s", err)
	}
	if _, err := buf.Exec(base, 8, 10, 12); err != nil {
		t.Fatalf("Error adding (8,10,12) to buffer: %s", err)
	}

	pass := testEx.checkLast(base+" (?,?,?),(?,?,?);", 1, 2, 3, 4, 5, 6)
	if !pass {
		t.Logf("Expected: %s %v", base+" (?,?,?),(?,?,?);", []int{1, 2, 3, 4, 5, 6})
		t.Fatalf("Received: %s %v", testEx.lastStmt, testEx.lastVals)
	}

	if err := buf.Commit(); err != nil {
		t.Fatalf("Error flushing buffer: %s", err)
	}

	pass = testEx.checkLast(base+" (?,?,?);", 8, 10, 12)
	if !pass {
		t.Logf("Expected: %s %v", base+" (?,?,?);", []int{8, 10, 12})
		t.Fatalf("Received: %s %v", testEx.lastStmt, testEx.lastVals)
	}
}

// TestTimedBuffer will check if the TimedBuffer flushes with the timeout.
func TestTimedBuffer(t *testing.T) {
	base := "INSERT INTO timed VALUES"
	testEx := &TestExecutor{t: t}
	buf := NewInsertBuffer(testEx, 2, false)
	tbuf := NewTimedBuffer(buf, 3*time.Second)

	if _, err := tbuf.Exec(base, 11, 13, 15); err != nil {
		t.Fatalf("Error adding (11, 13, 15) to TimedBuffer: %s", err)
	}
	if _, err := tbuf.Exec(base, 17, 19, 21); err != nil { // Should get flushed here
		t.Fatalf("Error adding (17, 19, 21) to TimedBuffer: %s", err)
	}
	if _, err := tbuf.Exec(base, 23, 25, 27); err != nil { // These values should stay in the buffer until at least 3 seconds have passed
		t.Fatalf("Error adding (23, 25, 27) to TimedBuffer: %s", err)
	}

	// Sleep for just 1 second, the 3rd set of values shouldn't have flushed yet.
	time.Sleep(1 * time.Second)
	pass := testEx.checkLast(base+" (?,?,?),(?,?,?);", 11, 13, 15, 17, 19, 21)
	if !pass {
		t.Logf("Expected: %s %v", base+" (?,?,?),(?,?,?);", []int{11, 13, 15, 17, 19, 21})
		t.Fatalf("Received: %s %v", testEx.lastStmt, testEx.lastVals)
	}

	// Sleep for three more seconds, everything should have flushed by now.
	time.Sleep(3 * time.Second)
	pass = testEx.checkLast(base+" (?,?,?);", 23, 25, 27)
	if !pass {
		t.Logf("Expected: %s %v", base+" (?,?,?);", []int{23, 25, 27})
		t.Fatalf("Received: %s %v", testEx.lastStmt, testEx.lastVals)
	}

	tbuf.Stop()
}

// TestBufferOnDb will connect to a live postgres instance to test the insert
// buffer.
func TestBufferOnDb(t *testing.T) {
	if testing.Short() {
		t.Skipf("Skipping TestBufferOnDb on short tests")
	}

	dbConn, err := getDbConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer dbConn.Close()

	if err := setupTestTable(dbConn); err != nil {
		t.Fatal(err)
	}

	t.Run("", func(t *testing.T) {
		dbBufferTest(t, dbConn)
	})

	if err := teardownTestTable(dbConn); err != nil {
		t.Fatal(err)
	}
}

func setupTestTable(db *sql.DB) error {
	stmt := "CREATE TABLE IF NOT EXISTS test (a int, b int, c int);"
	_, err := db.Exec(stmt)
	return err
}

func sumTestTable(db *sql.DB) (int, error) {
	queryStmt := "SELECT * FROM test;"
	rows, err := db.Query(queryStmt)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	sum := 0
	for rows.Next() {
		a, b, c := 0, 0, 0
		rows.Scan(&a, &b, &c)
		sum += a + b + c
	}
	return sum, nil
}

func teardownTestTable(db *sql.DB) error {
	stmt := "DROP TABLE test;"
	_, err := db.Exec(stmt)
	return err
}

// dbBufferTest will check the InsertBuffer on an actual DB connection.
func dbBufferTest(t *testing.T, db *sql.DB) {
	baseStmt := "INSERT INTO test VALUES"
	buf := NewInsertBuffer(db, 2, true)

	sum := 0
	if _, err := buf.Exec(baseStmt, 21, 22, 23); err != nil {
		t.Fatalf("Error adding (21, 22, 23) to DB: %s", err)
	}
	sum += 21 + 22 + 23
	if _, err := buf.Exec(baseStmt, 34, 35, 36); err != nil {
		t.Fatalf("Error adding (34, 35, 36) to DB: %s", err)
	}
	sum += 34 + 35 + 36
	if _, err := buf.Exec(baseStmt, 47, 48, 49); err != nil {
		t.Fatalf("Error adding (47, 48, 49) to DB: %s", err)
	}
	sum += 47 + 48 + 49
	if err := buf.Commit(); err != nil {
		t.Fatalf("Error flushing to DB: %s", err)
	}

	dbSum, err := sumTestTable(db)
	if err != nil {
		t.Fatal(err)
	} else if dbSum != sum {
		t.Fatalf("Expected: %d, Got: %d", sum, dbSum)
	}
}

func getDbConnection() (*sql.DB, error) {
	pgConstr := "user=bgpmon password=bgpmon dbname=bgpmon host=localhost sslmode=disable"
	return sql.Open("postgres", pgConstr)
}

// TestBufferBatchSize checks there will be an error if a batch size rule is violated.
func TestBufferBatchSize(t *testing.T) {
	testEx := &TestExecutor{t: t}
	buf := NewInsertBuffer(testEx, 2, false)
	if _, err := buf.Exec("", 1, 2, 3); err != nil {
		t.Fatalf("Error adding (1, 2, 3) for batch size 3: %s", err)
	}
	if _, err := buf.Exec("", 4, 5, 6); err != nil {
		t.Fatalf("Error adding (4, 5, 6) for batch size 3: %s", err)
	}
	if _, err := buf.Exec("", 6, 7, 8, 9, 10); err == nil {
		t.Fatal("Error expected but not received")
	}
	if _, err := buf.Exec("", 11, 12); err == nil {
		t.Fatal("Error expected but not received")
	}
}

func TestBufferQuery(t *testing.T) {
	testEx := &TestExecutor{t: t}
	buf := NewInsertBuffer(testEx, 2, false)

	stmt1 := "INSERT INTO test1"
	stmt2 := "INSERT INTO test2"

	if _, err := buf.Exec(stmt1, 1, 2); err != nil {
		t.Fatal(err)
	}

	if _, err := buf.Exec(stmt2, 2, 3); err == nil {
		t.Fatalf("Expected new statement error")
	}

	if _, err := buf.Exec(stmt1, 1, 2, 3); err == nil {
		t.Fatalf("Expected batch size error")
	}
}
