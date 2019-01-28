package db

import (
	"fmt"
	"github.com/CSUNetSec/bgpmon/v2/config"
	"github.com/CSUNetSec/bgpmon/v2/util"
	"github.com/lib/pq"
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
func checkSchema(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	csquery := ex.getdbop(CHECK_SCHEMA)
	var (
		res bool
		err error
	)

	tocheck := []string{msg.GetMainTable(), msg.GetNodeTable()}
	allgood := true
	for _, tname := range tocheck {
		if err = ex.QueryRow(csquery, tname).Scan(&res); err != nil {
			return NewReply(errors.Wrap(err, "checkSchema"))
		}
		dblogger.Infof("table:%s exists:%v", tname, res)
		allgood = allgood && res
	}

	if !allgood {
		return NewReply(errNoTable)
	}

	return NewReply(nil)
}

// syncNodes finds all the known nodes in the db, and composes them with the incoming nodes
// it then returns back the aggregate. If a node exists in both the incoming (from config)
// view is preffered.
func syncNodes(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	nodesMsg := msg.(nodesMessage)

	selectNodeTmpl := ex.getdbop(SELECT_NODE)
	insertNodeTmpl := ex.getdbop(INSERT_NODE)
	dbNodes := make(map[string]config.NodeConfig) //this keeps nodeconfigs recovered from the db
	cn := newNode()                               //the current node we will be looping over
	rows, err := ex.Query(fmt.Sprintf(selectNodeTmpl, nodesMsg.GetNodeTable()))
	if err != nil {
		dblogger.Errorf("syncNode query: %v", err)
		return NewNodesReply(nil, err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&cn.nodeName, &cn.nodeIP, &cn.nodeCollector, &cn.nodeDuration, &cn.nodeDescr, &cn.nodeCoords, &cn.nodeAddress)
		if err != nil {
			dblogger.Errorf("syncnode fetch node row:%s", err)
			return NewNodesReply(nil, err)
		}
		hereNewNodeConf := cn.nodeConfigFromNode()
		dbNodes[hereNewNodeConf.IP] = hereNewNodeConf
	}
	dblogger.Infof("calling sumnodes. known:%v, db:%v", nodesMsg.GetNodes(), dbNodes)
	allNodes := util.SumNodeConfs(nodesMsg.GetNodes(), dbNodes)
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
	return NewNodesReply(allNodes, nil)
}

//returns the first matching node from the db table based on ip or name
func getNode(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	nodeMsg := msg.(nodeMessage)

	selectNodeTmpl := ex.getdbop(SELECT_NODE)
	cn := newNode()
	rows, err := ex.Query(fmt.Sprintf(selectNodeTmpl, nodeMsg.GetNodeTable()))
	if err != nil {
		dblogger.Errorf("getNode query error:%s", err)
		return NewNodeReply(nil, err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&cn.nodeName, &cn.nodeIP, &cn.nodeCollector, &cn.nodeDuration, &cn.nodeDescr, &cn.nodeCoords, &cn.nodeAddress)
		if err != nil {
			dblogger.Errorf("getNode fetch node row:%s", err)
			return NewNodeReply(nil, err)
		}
		//try to match the node and ignore unset strings coming from sqlin
		dblogger.Infof("trying node matching with name:%s ip:%s", cn.nodeName, cn.nodeIP)
		name, ip := nodeMsg.GetNodeName(), nodeMsg.GetNodeIP()
		if (name == cn.nodeName && name != "") || (ip == cn.nodeIP && ip != "") {
			return NewNodeReply(cn, nil)
		}
	}

	return NewNodeReply(nil, errNoNode)
}

//creates a table to hold captures and registers it in the main table and the current known tables in memory.
func createCaptureTable(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	cMsg := msg.(capTableMessage)

	createCapTmpl := ex.getdbop(MAKE_CAPTURE_TABLE)
	name := cMsg.GetTableName()
	q := fmt.Sprintf(createCapTmpl, name)
	_, err := ex.Exec(q)
	if err != nil {
		dblogger.Errorf("createCaptureTable error:%s on command :%s", err, q)
		return NewCapTableReply("", "", time.Now(), time.Now(), err)
	}

	insertCapTmpl := ex.getdbop(INSERT_MAIN_TABLE)
	ip := cMsg.GetTableCol()
	sdate, edate := cMsg.GetDates()
	_, err = ex.Exec(fmt.Sprintf(insertCapTmpl, cMsg.GetMainTable()), name, ip, sdate, edate)

	if err != nil {
		dblogger.Errorf("createCaptureTable insertnode error:%s", err)
		return NewCapTableReply("", "", time.Now(), time.Now(), err)
	} else {
		dblogger.Infof("inserted table:%s", name)
	}

	return NewCapTableReply(name, ip, sdate, edate, nil)
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
	selectTableTmpl := ex.getdbop(SELECT_TABLE)
	cd := tMsg.GetColDate()
	qdate := cd.dat.UTC() //XXX this cast to utc is important. the db is dumb and doesn't figure it out. we need a better approach.
	rows, err := ex.Query(fmt.Sprintf(selectTableTmpl, tMsg.GetMainTable(), tMsg.GetNodeTable()), qdate, cd.col)
	if err != nil {
		dblogger.Errorf("getTable query error:%s", err)
		return NewTableReply("", time.Now(), time.Now(), nil, err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&resdbname, &rescollector, &restStart, &restEnd, &resdur)
		if err != nil {
			dblogger.Errorf("getNode fetch node row:%s", err)
			return NewTableReply("", time.Now(), time.Now(), nil, err)
		}
		//we found a table for that range.
		n := &node{nodeIP: cd.col, nodeName: rescollector, nodeDuration: resdur}
		return NewTableReply(resdbname, restStart, restEnd, n, nil)
	}

	return NewTableReply("", time.Now(), time.Now(), nil, errNoTable)
}

// creates the necessary bgpmon schema, if the tables don't exist
func makeSchema(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	maintableTmpl := ex.getdbop(MAKE_MAIN_TABLE)
	nodetableTmpl := ex.getdbop(MAKE_NODE_TABLE)
	var (
		err error
	)
	if _, err = ex.Exec(fmt.Sprintf(maintableTmpl, msg.GetMainTable())); err != nil {
		return NewReply(errors.Wrap(err, "makeSchema maintable"))
	}
	dblogger.Infof("created table:%s", msg.GetMainTable())

	if _, err = ex.Exec(fmt.Sprintf(nodetableTmpl, msg.GetNodeTable())); err != nil {
		return NewReply(errors.Wrap(err, "makeSchema nodetable"))
	}
	dblogger.Infof("created table:%s", msg.GetNodeTable())
	return NewReply(nil)
}

func insertCapture(ex SessionExecutor, msg CommonMessage) CommonReply {
	cMsg := msg.(captureMessage)

	insertTmpl := ex.getdbop(INSERT_CAPTURE_TABLE)
	stmt := fmt.Sprintf(insertTmpl, cMsg.GetTableName())

	advPrefixes, _ := util.GetAdvertizedPrefixes(cMsg.GetCapture())
	wdrPrefixes, _ := util.GetWithdrawnPrefixes(cMsg.GetCapture())

	adv := util.PrefixesToPQArray(advPrefixes)
	wdr := util.PrefixesToPQArray(wdrPrefixes)

	time, colIP, _ := util.GetTimeColIP(cMsg.GetCapture())
	peerIP, _ := util.GetPeerIP(cMsg.GetCapture())
	asPath := util.GetAsPath(cMsg.GetCapture())
	nextHop, _ := util.GetNextHop(cMsg.GetCapture())
	// This util function needs to be fixed
	origin := util.GetOriginAs(cMsg.GetCapture())
	protoMsg := util.GetProtoMsg(cMsg.GetCapture())

	_, err := ex.Exec(stmt, time, colIP.String(), peerIP.String(), pq.Array(asPath), nextHop.String(), origin, adv, wdr, protoMsg)
	if err != nil {
		dblogger.Infof("failing to insert capture: time:%s colip:%s  aspath:%v oas:%v ", time, colIP.String(), asPath, origin)
		return NewReply(errors.Wrap(err, "insertCapture"))
	}

	return NewReply(nil)
}
