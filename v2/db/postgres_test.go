package db

import (
	"database/sql"
	"github.com/CSUNetSec/bgpmon/v2/config"
	"testing"
)

const (
	pgconstr = "user=bgpmon password=bgpmon dbname=bgpmon host=localhost sslmode=disable"
)

var (
	cn1 = config.NodeConfig{
		IP:          "1.2.3.4",
		Name:        "name1",
		IsCollector: true,
		Description: "lalalacollector",
	}
	cn2 = config.NodeConfig{
		IP:          "3.4.4.4",
		Name:        "name2",
		IsCollector: false,
		Description: "lalalapeer",
	}
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
	csargs := sqlIn{dbname: "bgpmon", maintable: "dbs", nodetable: "nodes"}
	t.Log("postgres opened for makeschema test")
	if ok, err := retCheckSchema(checkSchema(db, csargs)); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("check was :%v", ok)
	}
	if err := retMakeSchema(makeSchema(db, csargs)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncNodes(t *testing.T) {
	db, err := sql.Open("postgres", pgconstr)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	innodes := make(map[string]config.NodeConfig)
	innodes[cn1.IP] = cn1
	innodes[cn2.IP] = cn2
	sin := sqlIn{dbname: "bgpmon", nodetable: "nodes", knownNodes: innodes}
	syncNodes(db, sin)

}
