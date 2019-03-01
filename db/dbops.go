package db

import (
	"fmt"
	"time"

	"github.com/CSUNetSec/bgpmon/config"
	"github.com/CSUNetSec/bgpmon/util"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

var (
	errNoNode  = errors.New("no such node in DB")
	errNoTable = errors.New("no such table in DB")
)

//DB Operations
//these require the sql executor to be already set up for them.
//checkschema makes sure that all the required tables exist in the database
func checkSchema(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	csquery := ex.getdbop(checkSchemaOp)
	var (
		res bool
		err error
	)

	tocheck := []string{msg.GetMainTable(), msg.GetNodeTable()}
	allgood := true
	for _, tname := range tocheck {
		if err = ex.QueryRow(csquery, tname).Scan(&res); err != nil {
			return newReply(errors.Wrap(err, "checkSchema"))
		}
		dblogger.Infof("table:%s exists:%v", tname, res)
		allgood = allgood && res
	}

	if !allgood {
		return newReply(errNoTable)
	}

	return newReply(nil)
}

//syncNodes finds all the known nodes in the db, and composes them with the incoming nodes
//it then returns back the aggregate. If a node exists in both the incoming (from config)
//view is preffered.
func syncNodes(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	nodesMsg := msg.(nodesMessage)

	selectNodeTmpl := ex.getdbop(selectNodeOp)
	insertNodeTmpl := ex.getdbop(insertNodeOp)
	dbNodes := make(map[string]config.NodeConfig) //this keeps nodeconfigs recovered from the db
	cn := newNode()                               //the current node we will be looping over
	rows, err := ex.Query(fmt.Sprintf(selectNodeTmpl, nodesMsg.GetNodeTable()))
	if err != nil {
		dblogger.Errorf("syncNode query: %v", err)
		return newNodesReply(nil, err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&cn.nodeName, &cn.nodeIP, &cn.nodeCollector, &cn.nodeDuration, &cn.nodeDescr, &cn.nodeCoords, &cn.nodeAddress)
		if err != nil {
			dblogger.Errorf("syncnode fetch node row:%s", err)
			return newNodesReply(nil, err)
		}
		hereNewNodeConf := cn.nodeConfigFromNode()
		dbNodes[hereNewNodeConf.IP] = hereNewNodeConf
	}
	dblogger.Infof("calling sumnodes. known:%v, db:%v", nodesMsg.getNodes(), dbNodes)
	allNodes := config.SumNodeConfs(nodesMsg.getNodes(), dbNodes)
	for _, v := range allNodes {
		_, err := ex.Exec(fmt.Sprintf(insertNodeTmpl, nodesMsg.GetNodeTable()),
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
	return newNodesReply(allNodes, nil)
}

//returns the first matching node from the db table based on ip or name
func getNode(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	nodeMsg := msg.(nodeMessage)

	selectNodeTmpl := ex.getdbop(selectNodeOp)
	cn := newNode()
	rows, err := ex.Query(fmt.Sprintf(selectNodeTmpl, nodeMsg.GetNodeTable()))
	if err != nil {
		dblogger.Errorf("getNode query error:%s", err)
		return newNodeReply(nil, err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&cn.nodeName, &cn.nodeIP, &cn.nodeCollector, &cn.nodeDuration, &cn.nodeDescr, &cn.nodeCoords, &cn.nodeAddress)
		if err != nil {
			dblogger.Errorf("getNode fetch node row:%s", err)
			return newNodeReply(nil, err)
		}
		//try to match the node and ignore unset strings coming from sqlin
		dblogger.Infof("trying node matching with name:%s ip:%s", cn.nodeName, cn.nodeIP)
		name, ip := nodeMsg.getNodeName(), nodeMsg.getNodeIP()
		if (name == cn.nodeName && name != "") || (ip == cn.nodeIP && ip != "") {
			return newNodeReply(cn, nil)
		}
	}

	return newNodeReply(nil, errNoNode)
}

//creates a table to hold captures and registers it in the main table and the current known tables in memory.
func createCaptureTable(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	cMsg := msg.(capTableMessage)

	createCapTmpl := ex.getdbop(makeCaptureTableOp)
	name := cMsg.getTableName()
	q := fmt.Sprintf(createCapTmpl, name)
	_, err := ex.Exec(q)
	if err != nil {
		dblogger.Errorf("createCaptureTable error:%s on command :%s", err, q)
		return newCapTableReply("", "", time.Now(), time.Now(), err)
	}

	insertCapTmpl := ex.getdbop(insertMainTableOp)
	ip := cMsg.getTableCol()
	sdate, edate := cMsg.getDates()
	_, err = ex.Exec(fmt.Sprintf(insertCapTmpl, cMsg.GetMainTable()), name, ip, sdate, edate)

	if err != nil {
		dblogger.Errorf("createCaptureTable insertnode error:%s", err)
		return newCapTableReply("", "", time.Now(), time.Now(), err)
	}
	dblogger.Infof("inserted table:%s", name)

	return newCapTableReply(name, ip, sdate, edate, nil)
}

//returns the collector table from the main dbs table
func getTable(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	tMsg := msg.(tableMessage)

	var (
		resdbname    string
		rescollector string
		restStart    time.Time
		restEnd      time.Time
		resdur       int
	)
	selectTableTmpl := ex.getdbop(selectTableOp)
	cd := tMsg.getColDate()
	qdate := cd.dat.UTC() //XXX this cast to utc is important. the db is dumb and doesn't figure it out. we need a better approach.
	rows, err := ex.Query(fmt.Sprintf(selectTableTmpl, tMsg.GetMainTable(), tMsg.GetNodeTable()), qdate, cd.col)
	if err != nil {
		dblogger.Errorf("getTable query error:%s", err)
		return newTableReply("", time.Now(), time.Now(), nil, err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&resdbname, &rescollector, &restStart, &restEnd, &resdur)
		if err != nil {
			dblogger.Errorf("getNode fetch node row:%s", err)
			return newTableReply("", time.Now(), time.Now(), nil, err)
		}
		//we found a table for that range.
		n := &node{nodeIP: cd.col, nodeName: rescollector, nodeDuration: resdur}
		return newTableReply(resdbname, restStart, restEnd, n, nil)
	}

	return newTableReply("", time.Now(), time.Now(), nil, errNoTable)
}

// creates the necessary bgpmon schema, if the tables don't exist
func makeSchema(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	maintableTmpl := ex.getdbop(makeMainTableOp)
	nodetableTmpl := ex.getdbop(makeNodeTableOp)
	var (
		err error
	)
	if _, err = ex.Exec(fmt.Sprintf(maintableTmpl, msg.GetMainTable())); err != nil {
		return newReply(errors.Wrap(err, "makeSchema maintable"))
	}
	dblogger.Infof("created table:%s", msg.GetMainTable())

	if _, err = ex.Exec(fmt.Sprintf(nodetableTmpl, msg.GetNodeTable())); err != nil {
		return newReply(errors.Wrap(err, "makeSchema nodetable"))
	}
	dblogger.Infof("created table:%s", msg.GetNodeTable())
	return newReply(nil)
}

func insertCapture(ex SessionExecutor, msg CommonMessage) CommonReply {
	cMsg := msg.(captureMessage)

	insertTmpl := ex.getdbop(insertCaptureTableOp)
	stmt := fmt.Sprintf(insertTmpl, cMsg.getTableName())

	advPrefixes, _ := util.GetAdvertizedPrefixes(cMsg.getCapture())
	wdrPrefixes, _ := util.GetWithdrawnPrefixes(cMsg.getCapture())

	adv := util.PrefixesToPQArray(advPrefixes)
	wdr := util.PrefixesToPQArray(wdrPrefixes)

	time, colIP, _ := util.GetTimeColIP(cMsg.getCapture())
	peerIP, _ := util.GetPeerIP(cMsg.getCapture())
	asPath := util.GetAsPath(cMsg.getCapture())
	nextHop, _ := util.GetNextHop(cMsg.getCapture())
	// This util function needs to be fixed
	origin := util.GetOriginAs(cMsg.getCapture())
	protoMsg := util.GetProtoMsg(cMsg.getCapture())

	_, err := ex.Exec(stmt, time, colIP.String(), peerIP.String(), pq.Array(asPath), nextHop.String(), origin, adv, wdr, protoMsg)
	if err != nil {
		dblogger.Infof("failing to insert capture: time:%s colip:%s  aspath:%v oas:%v ", time, colIP.String(), asPath, origin)
		return newReply(errors.Wrap(err, "insertCapture"))
	}

	return newReply(nil)
}

//the reason that i'm implementing getCaptures to manually iterate on
//all tables and not to use a dbfunction that via dynamic sql would return
//the result, is that i am worried that someone might request too many captures
//and i would like the return to be streamed. therefore i iterate on the table and
//create a common replies. it also differs in the sense that it has a channel reply
func getCaptures(ex SessionExecutor, msg CommonMessage) chan CommonReply {
	retc := make(chan CommonReply)
	go func(SessionExecutor, CommonMessage, chan CommonReply) {
		defer close(retc)
		var tablename string
		cMsg := msg.(getCapMessage)
		getCapTablesTmpl := ex.getdbop(getCaptureTablesOp)
		stmt := fmt.Sprintf(getCapTablesTmpl, msg.GetMainTable())
		stime, etime := cMsg.getDates()
		rows, err := ex.Query(stmt, cMsg.getTableCol(), stime, etime)
		if err != nil {
			dblogger.Errorf("getTable query error:%s", err)
			retc <- newGetCapReply(nil, err)
			return
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&tablename)
			if err != nil {
				dblogger.Errorf("getCaptures can't find tables to scan:%s", err)
				retc <- newGetCapReply(nil, err)
				return
			}
			dblogger.Infof("opening table:%s to get captures", tablename)
			//XXX implement

		}
	}(ex, msg, retc)
	return retc
}
