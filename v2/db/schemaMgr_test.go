package db

import (
	"database/sql"
	_ "github.com/lib/pq"
	"testing"
)

var (
	gex SessionExecutor       // global session executor for tests
	gdb *sql.DB         = nil // global sql db
)

func getex() (SessionExecutor, *sql.DB) {
	if gdb != nil {
		return gex, gdb
	}
	pgconstr := "user=bgpmon password=bgpmon dbname=bgpmon host=localhost sslmode=disable"
	db, err := sql.Open("postgres", pgconstr)
	if err != nil {
		panic(err)
	}
	gdb = db
	dbo := newPostgressDbOper()
	sex := newDbSessionExecutor(db, dbo)
	gex = sex
	return sex, db
}

func TestSchemaMgrStartStop(t *testing.T) {
	sx, _ := getex()
	sm := newSchemaMgr(sx)
	go sm.Run()
	sm.Close()
}

func TestXXXClose(t *testing.T) {
	//just a null test to close the db connection
	gdb.Close()
}
