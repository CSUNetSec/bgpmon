package db

import (
	"fmt"
	"github.com/CSUNetSec/bgpmon/v2/config"
	"github.com/CSUNetSec/bgpmon/v2/util"
	"github.com/pkg/errors"
	"time"
)

var (
	errNoNode  = errors.New("no such node in DB")
	errNoTable = errors.New("no such table in DB")
)

// DB Operations
// these require the sql executor to be already set up for them.
// checkschema makes sure that all the required tables exist in the database
func checkSchema(ex SessionExecutor, args sqlIn) (ret sqlOut) {
	csquery := ex.getdbop(CHECK_SCHEMA)
	var (
		res bool
		err error
	)
	tocheck := []string{args.maintable, args.nodetable}
	allgood := true
	for _, tname := range tocheck {
		if err = ex.QueryRow(csquery, tname).Scan(&res); err != nil {
			ret.ok, ret.err = false, errors.Wrap(err, "checkSchema")
			return
		}
		dblogger.Infof("table:%s exists:%v", tname, res)
		allgood = allgood && res
	}
	ret.ok, ret.err = allgood, nil
	return
}

// syncNodes finds all the known nodes in the db, and composes them with the incoming nodes
// it then returns back the aggregate. If a node exists in both the incoming (from config)
// view is preffered.
func syncNodes(ex SessionExecutor, args sqlIn) (ret sqlOut) {
	selectNodeTmpl := ex.getdbop(SELECT_NODE)
	insertNodeTmpl := ex.getdbop(INSERT_NODE)
	dbNodes := make(map[string]config.NodeConfig) //this keeps nodeconfigs recovered from the db
	cn := newNode()                               //the current node we will be looping over
	rows, err := ex.Query(fmt.Sprintf(selectNodeTmpl, args.nodetable))
	if err != nil {
		dblogger.Errorf("syncNode query:", err)
		ret.err = err
		return
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(cn.nodeName, cn.nodeIP, cn.nodeCollector, cn.nodeDuration, cn.nodeDescr, cn.nodeCoords, cn.nodeAddress)
		if err != nil {
			dblogger.Errorf("syncnode fetch node row:%s", err)
			ret.err = err
			return
		}
		hereNewNodeConf := cn.nodeConfigFromNode()
		dbNodes[hereNewNodeConf.IP] = hereNewNodeConf
	}
	allNodes := util.SumNodeConfs(args.knownNodes, dbNodes)
	for _, v := range allNodes {
		_, err := ex.Exec(fmt.Sprintf(insertNodeTmpl, args.nodetable),
			v.Name,
			v.IP,
			v.IsCollector,
			v.DumpDurationMinutes,
			v.Description,
			v.Coords,
			v.Location)
		if err != nil {
			dblogger.Errorf("failed to insert node config. %s", err)
		} else {
			dblogger.Infof("inserted node config. %v", v)
		}
	}
	ret.knownNodes = allNodes
	return
}

//returns the first matching node from the db table based on ip or name
func getNode(ex SessionExecutor, args sqlIn) (ret sqlOut) {
	selectNodeTmpl := ex.getdbop(SELECT_NODE)
	cn := newNode()
	rows, err := ex.Query(fmt.Sprintf(selectNodeTmpl, args.nodetable))
	if err != nil {
		dblogger.Errorf("getNode query error:%s", err)
		ret.err = err
		return
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&cn.nodeName, &cn.nodeIP, &cn.nodeCollector, &cn.nodeDuration, &cn.nodeDescr, &cn.nodeCoords, &cn.nodeAddress)
		if err != nil {
			dblogger.Errorf("getNode fetch node row:%s", err)
			ret.err = err
			return
		}
		//try to match the node and ignore unset strings coming from sqlin
		if (args.getNodeName == cn.nodeName && args.getNodeName != "") || (args.getNodeIP == cn.nodeIP && args.getNodeIP != "") {
			ret.resultNode = cn
			return
		}
	}
	ret.err = errNoNode
	return
}

//creates a table to hold captures and registers it in the main table and the current known tables in memory.
func createCaptureTable(ex SessionExecutor, args sqlIn) (ret sqlOut) {
	createCapTmpl := ex.getdbop(MAKE_CAPTURE_TABLE)
	name := args.capTableName
	q := fmt.Sprintf(createCapTmpl, name)
	_, err := ex.Query(q)
	if err != nil {
		dblogger.Errorf("createCaptureTable error:%s on command :%s", err, q)
		ret.err = err
		return
	}
	insertCapTmpl := ex.getdbop(INSERT_MAIN_TABLE)
	ip := args.capTableCol
	sdate := args.capTableSdate
	edate := args.capTableEdate
	row, err := ex.Query(fmt.Sprintf(insertCapTmpl, args.maintable), name, ip, sdate, edate)
	if err != nil {
		dblogger.Errorf("createCaptureTable insertnode error:%s", err)
		ret.err = err
		return
	} else {
		dblogger.Infof("inserted table:%s at row:%v", name, row)
	}
	ret.capTable, ret.capIp, ret.capStime, ret.capEtime = name, ip, sdate, edate
	return
}

//returns the collector table from the main dbs table
func getTable(ex SessionExecutor, args sqlIn) (ret sqlOut) {
	var (
		resdbname    string
		rescollector string
		restStart    time.Time
		restEnd      time.Time
	)
	selectTableTmpl := ex.getdbop(SELECT_TABLE)
	qdate := args.getColDate.dat.UTC() //XXX this cast to utc is important. the db is dumb and doesn't figure it out. we need a better approach.
	rows, err := ex.Query(fmt.Sprintf(selectTableTmpl, args.maintable), qdate)
	if err != nil {
		dblogger.Errorf("getTable query error:%s", err)
		ret.err = err
		return
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&resdbname, &rescollector, &restStart, &restEnd)
		if err != nil {
			dblogger.Errorf("getNode fetch node row:%s", err)
			ret.err = err
			return
		}
		dblogger.Printf("got dbname:%s collector:%s tstart:%s tend:%s", resdbname, rescollector, restStart, restEnd)
		//we found a table for that range.
		ret.capTable, ret.capIp, ret.capStime, ret.capEtime = resdbname, rescollector, restStart, restEnd
		return
	}
	ret.err = errNoTable
	return
}

// creates the necessary bgpmon schema, if the tables don't exist
func makeSchema(ex SessionExecutor, args sqlIn) (ret sqlOut) {
	maintableTmpl := ex.getdbop(MAKE_MAIN_TABLE)
	nodetableTmpl := ex.getdbop(MAKE_NODE_TABLE)
	var (
		err error
	)
	if _, err = ex.Exec(fmt.Sprintf(maintableTmpl, args.maintable)); err != nil {
		ret.err = errors.Wrap(err, "makeSchema maintable")
		return
	}
	dblogger.Infof("created table:%s", args.maintable)

	if _, err = ex.Exec(fmt.Sprintf(nodetableTmpl, args.nodetable)); err != nil {
		ret.err = errors.Wrap(err, "makeSchema nodetable")
		return
	}
	dblogger.Infof("created table:%s", args.nodetable)
	return
}

func retCheckSchema(o sqlOut) (bool, error) {
	return o.ok, o.err
}

func retMakeSchema(o sqlOut) error {
	return o.err
}
