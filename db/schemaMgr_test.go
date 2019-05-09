package db

import (
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
)

var (
	gEx SessionExecutor // global session executor for tests
	gDB *sql.DB         // global sql db
)

func getEx() (SessionExecutor, *sql.DB) {
	if gDB != nil {
		return gEx, gDB
	}
	db, err := sql.Open("postgres", pgConstr)
	if err != nil {
		panic(err)
	}
	gDB = db
	provider := newPostgressQueryProvider()
	sEx := newSessionExecutor(db, provider)
	gEx = sEx
	return sEx, db
}

func TestSchemaMgrStartStop(t *testing.T) {
	if testing.Short() {
		t.Skipf("Skipping TestSchemaMgr for short tests")
	}
	sx, _ := getEx()
	sm := newSchemaMgr(sx, "dbs", "nodes", "entities")
	sm.stop()
	t.Log("schema mgr started and closed")
}

func TestSchemaCheckSchema(t *testing.T) {
	if testing.Short() {
		t.Skipf("Skipping TestSchemaCheckSchema for short tests")
	}
	sx, _ := getEx()
	sm := newSchemaMgr(sx, "dbs", "nodes", "entities")

	ok, err := sm.checkSchema()
	t.Logf("schema mgr checkSchema: [ok:%v , err:%v]", ok, err)
	sm.stop()
	t.Log("schema mgr started and closed")
}

// Just a null test to close the db connection
func TestXXXClose(t *testing.T) {
	if testing.Short() {
		t.Skipf("Skipping TestXXXClose for short tests")
	}

	if err := gDB.Close(); err != nil {
		t.Fatal(err)
	}
}
