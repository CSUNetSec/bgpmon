package db

import (
	"database/sql"
	"github.com/CSUNetSec/bgpmon/config"
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
	pdboper = newPostgressDbOper()
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
	msg := newCustomMessage("dbs", "nodes")
	t.Log("postgres opened for makeschema test")
	if err := checkSchema(sex, msg).Error(); err != nil && err != errNoTable {
		t.Fatal(err)
	} else if err == errNoTable {
		t.Logf("Tables not found")
	} else {
		t.Logf("Tables found")
	}

	if err := makeSchema(sex, msg).Error(); err != nil {
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
	msg := newNodesMessage(innodes)
	msg.SetNodeTable("nodes")
	rep := syncNodes(sex, msg)

	if rep.Error() != nil {
		t.Fatal(rep.Error())
	}
	nRep := rep.(nodesReply)
	config.PutConfiguredNodes(nRep.GetNodes(), os.Stdout)
}
