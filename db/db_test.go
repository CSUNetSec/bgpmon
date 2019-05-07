package db

// These tests, like many of the test files contained in the db/ directory, require
// a running postgresql instance with a bgpmon user and database.

import (
	"database/sql"
	"testing"

	"github.com/CSUNetSec/bgpmon/config"
)

const (
	pgConstr = "user=bgpmon password=bgpmon dbname=bgpmon host=localhost sslmode=disable"
)

// These are just dummy node configs to test their insertion and syncing.
var (
	cn1 = config.NodeConfig{
		IP:          "1.1.1.1",
		Name:        "name1",
		IsCollector: true,
		Description: "coming from config",
	}
	cn2 = config.NodeConfig{
		IP:          "2.2.2.2",
		Name:        "name2",
		IsCollector: false,
		Description: "also from config and updated",
	}
	cn3 = config.NodeConfig{
		IP:          "3.3.3.3",
		Name:        "name3",
		IsCollector: false,
		Description: "in the db and updated",
	}
	queries = newPostgressQueryProvider()
)

func TestConnectPostgres(t *testing.T) {
	if testing.Short() {
		t.Skipf("Skipping TestConnectPostgres for short tests")
	}
	db, err := sql.Open("postgres", pgConstr)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	t.Log("postgres pinged successfully")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMakeSchema(t *testing.T) {
	if testing.Short() {
		t.Skipf("Skipping TestMakeSchema for short tests")
	}

	db, err := sql.Open("postgres", pgConstr)
	if err != nil {
		t.Fatal(err)
	}
	// Although Close is called later, it is deferred here in case theres
	// a premature test failure.
	defer db.Close()
	sex := newSessionExecutor(db, queries)
	msg := newCustomMessage("dbs", "nodes", "entities")

	t.Log("postgres opened for makeschema test")
	if err := checkSchema(sEx, msg).Error(); err != nil && err != errNoTable {
		t.Fatal(err)
	} else if err == errNoTable {
		t.Logf("Tables not found")
	} else {
		t.Logf("Tables found")
	}

	if err := makeSchema(sEx, msg).Error(); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// This test has no automated fail condition, it must be checked manually by looking into
// the DB.
func TestSyncNodes(t *testing.T) {
	if testing.Short() {
		t.Skipf("Skipping TestSyncNodes for short tests")
	}

	insCmd := "INSERT INTO nodes (name, ip, isCollector, tableDumpDurationMinutes, description, coords, address) VALUES ($1, $2, $3, $4, $5, $6, $7);"
	db, err := sql.Open("postgres", pgConstr)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sEx := newSessionExecutor(db, queries)
	inNodes := make(map[string]config.NodeConfig)
	// Insert node 3 in the db so that syncNodes finds it there.
	// That will test merging of incoming and already there nodes.
	if _, err := db.Exec(insCmd, cn3.Name, cn3.IP, cn3.IsCollector, cn3.DumpDurationMinutes, cn3.Description, "", ""); err != nil {
		t.Logf("failed inserting test node in the db")
	}
	inNodes[cn1.IP] = cn1
	inNodes[cn2.IP] = cn2
	msg := newNodesMessage(inNodes)
	msg.SetNodeTable("nodes")
	rep := syncNodes(sEx, msg)

	if rep.Error() != nil {
		t.Fatal(rep.Error())
	}
}
