package db

import (
	"database/sql"
	"github.com/CSUNetSec/bgpmon/v2/config"
	"os"
	"testing"
)

const (
	pgconstr = "user=bgpmon password=bgpmon dbname=bgpmon host=localhost sslmode=disable"
)

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
	pdboper = newPostgressDboper()
)

func TestConnectPostgres(t *testing.T) {
	db, err := sql.Open("postgres", pgconstr)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	t.Log("postgres pinged succesfully")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMakeSchema(t *testing.T) {
	db, err := sql.Open("postgres", pgconstr)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sex := newDbSessionExecutor(db, pdboper)
	csargs := sqlIn{dbname: "bgpmon", maintable: "dbs", nodetable: "nodes"}
	t.Log("postgres opened for makeschema test")
	if ok, err := retCheckSchema(checkSchema(sex, csargs)); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("check was :%v", ok)
	}
	if err := retMakeSchema(makeSchema(sex, csargs)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncNodes(t *testing.T) {
	insCmd := "INSERT INTO nodes (name, ip, isCollector, tableDumpDurationMinutes, description, coords, address) VALUES ($1, $2, $3, $4, $5, $6, $7);"
	db, err := sql.Open("postgres", pgconstr)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sex := newDbSessionExecutor(db, pdboper)
	innodes := make(map[string]config.NodeConfig)
	//insert node 3 in the db so that syncnodes finds it there.
	//that will test merging of incoming and already there nodes
	if _, err := db.Exec(insCmd, cn3.Name, cn3.IP, cn3.IsCollector, cn3.DumpDurationMinutes, cn3.Description, "", ""); err != nil {
		t.Logf("failed inserting test node in the db")
	}
	innodes[cn1.IP] = cn1
	innodes[cn2.IP] = cn2
	sin := sqlIn{dbname: "bgpmon", nodetable: "nodes", knownNodes: innodes}
	sout := syncNodes(sex, sin)
	config.PutConfiguredNodes(sout.knownNodes, os.Stdout)
}
