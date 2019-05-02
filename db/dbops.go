package db

import (
	"context"
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
	csquery := ex.getQuery(checkSchemaOp)
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
		dbLogger.Infof("table:%s exists:%v", tname, res)
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

	selectNodeTmpl := ex.getQuery(selectNodeOp)
	insertNodeTmpl := ex.getQuery(insertNodeOp)
	dbNodes := make(map[string]config.NodeConfig) //this keeps nodeconfigs recovered from the db
	cn := newNode()                               //the current node we will be looping over
	rows, err := ex.Query(fmt.Sprintf(selectNodeTmpl, nodesMsg.GetNodeTable()))
	if err != nil {
		dbLogger.Errorf("syncNode query: %v", err)
		return newNodesReply(nil, err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&cn.name, &cn.ip, &cn.isCollector, &cn.duration, &cn.description, &cn.coords, &cn.address)
		if err != nil {
			dbLogger.Errorf("syncnode fetch node row:%s", err)
			return newNodesReply(nil, err)
		}
		hereNewNodeConf := cn.nodeConfigFromNode()
		dbNodes[hereNewNodeConf.IP] = hereNewNodeConf
	}
	dbLogger.Infof("calling sumnodes. known:%v, db:%v", nodesMsg.getNodes(), dbNodes)
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
			dbLogger.Errorf("failed to insert node config. %s", err)
		} else {
			dbLogger.Infof("inserted node config. %v", v)
		}
	}
	return newNodesReply(allNodes, nil)
}

//returns the first matching node from the db table based on ip or name
func getNode(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	nodeMsg := msg.(nodeMessage)

	selectNodeTmpl := ex.getQuery(selectNodeOp)
	cn := newNode()
	rows, err := ex.Query(fmt.Sprintf(selectNodeTmpl, nodeMsg.GetNodeTable()))
	if err != nil {
		dbLogger.Errorf("getNode query error:%s", err)
		return newNodeReply(nil, err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&cn.name, &cn.ip, &cn.isCollector, &cn.duration, &cn.description, &cn.coords, &cn.address)
		if err != nil {
			dbLogger.Errorf("getNode fetch node row:%s", err)
			return newNodeReply(nil, err)
		}
		//try to match the node and ignore unset strings coming from sqlin
		dbLogger.Infof("trying node matching with name:%s ip:%s", cn.name, cn.ip)
		name, ip := nodeMsg.getNodeName(), nodeMsg.getNodeIP()
		if (name == cn.name && name != "") || (ip == cn.ip && ip != "") {
			return newNodeReply(cn, nil)
		}
	}

	return newNodeReply(nil, errNoNode)
}

//creates a table to hold captures and registers it in the main table and the current known tables in memory.
func createCaptureTable(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	cMsg := msg.(capTableMessage)

	createCapTmpl := ex.getQuery(makeCaptureTableOp)
	name := cMsg.getTableName()
	q := fmt.Sprintf(createCapTmpl, name)
	_, err := ex.Exec(q)
	if err != nil {
		dbLogger.Errorf("createCaptureTable error:%s on command :%s", err, q)
		return newCapTableReply("", "", time.Now(), time.Now(), err)
	}

	insertCapTmpl := ex.getQuery(insertMainTableOp)
	ip := cMsg.getTableCol()
	sdate, edate := cMsg.getDates()
	_, err = ex.Exec(fmt.Sprintf(insertCapTmpl, cMsg.GetMainTable()), name, ip, sdate, edate)

	if err != nil {
		dbLogger.Errorf("createCaptureTable insertnode error:%s", err)
		return newCapTableReply("", "", time.Now(), time.Now(), err)
	}
	dbLogger.Infof("inserted table:%s", name)

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
	selectTableTmpl := ex.getQuery(selectTableOp)
	colIP := tMsg.getColIP()
	qdate := tMsg.getDate().UTC() //XXX this cast to utc is important. the db is dumb and doesn't figure it out. we need a better approach.
	rows, err := ex.Query(fmt.Sprintf(selectTableTmpl, tMsg.GetMainTable(), tMsg.GetNodeTable()), qdate, colIP)
	if err != nil {
		dbLogger.Errorf("getTable query error:%s", err)
		return newTableReply("", time.Now(), time.Now(), nil, err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&resdbname, &rescollector, &restStart, &restEnd, &resdur)
		if err != nil {
			dbLogger.Errorf("getNode fetch node row:%s", err)
			return newTableReply("", time.Now(), time.Now(), nil, err)
		}
		//we found a table for that range.
		n := &node{ip: colIP, name: rescollector, duration: resdur}
		return newTableReply(resdbname, restStart, restEnd, n, nil)
	}

	return newTableReply("", time.Now(), time.Now(), nil, errNoTable)
}

// creates the necessary bgpmon schema, if the tables don't exist
func makeSchema(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	maintableTmpl := ex.getQuery(makeMainTableOp)
	nodetableTmpl := ex.getQuery(makeNodeTableOp)
	var (
		err error
	)
	if _, err = ex.Exec(fmt.Sprintf(maintableTmpl, msg.GetMainTable())); err != nil {
		return newReply(errors.Wrap(err, "makeSchema maintable"))
	}
	dbLogger.Infof("created table:%s", msg.GetMainTable())

	if _, err = ex.Exec(fmt.Sprintf(nodetableTmpl, msg.GetNodeTable())); err != nil {
		return newReply(errors.Wrap(err, "makeSchema nodetable"))
	}
	dbLogger.Infof("created table:%s", msg.GetNodeTable())
	return newReply(nil)
}

func insertCapture(ex SessionExecutor, msg CommonMessage) CommonReply {
	cMsg := msg.(captureMessage)

	insertTmpl := ex.getQuery(insertCaptureTableOp)
	stmt := fmt.Sprintf(insertTmpl, cMsg.getTableName())

	advPrefixes, _ := util.GetAdvertisedPrefixes(cMsg.getCapture())
	wdrPrefixes, _ := util.GetWithdrawnPrefixes(cMsg.getCapture())

	adv := util.PrefixesToPQArray(advPrefixes)
	wdr := util.PrefixesToPQArray(wdrPrefixes)

	time, colIP, _ := util.GetTimeColIP(cMsg.getCapture())
	peerIP, errPeerIP := util.GetPeerIP(cMsg.getCapture())
	if errPeerIP != nil {
		return newReply(errors.Wrap(errPeerIP, "insertCapture with no peer"))
	}
	asPath, errASpath := util.GetASPath(cMsg.getCapture())
	if errASpath != nil && advPrefixes != nil {
		return newReply(errors.Wrap(errASpath, "insertCapture, advertised prefixes but no AS-path"))
	}
	nextHop, errNextHop := util.GetNextHop(cMsg.getCapture())
	if errNextHop != nil && advPrefixes != nil {
		return newReply(errors.Wrap(errNextHop, "insertCapture, advertised prefixes but no next hop"))
	}
	origin, errOrigin := util.GetOriginAS(cMsg.getCapture())
	if errOrigin != nil && advPrefixes != nil { // this case should be caught by errASpath but anyway.
		return newReply(errors.Wrap(errOrigin, "insertCapture, advertised prefixes but no origin AS"))
	}
	protoMsg := util.GetProtoMsg(cMsg.getCapture())

	_, err := ex.Exec(stmt, time, colIP.String(), peerIP.String(), pq.Array(asPath), nextHop.String(), origin, adv, wdr, protoMsg)
	if err != nil {
		dbLogger.Infof("failing to insert capture: time:%s colip:%s  aspath:%v oas:%v ", time, colIP.String(), asPath, origin)
		return newReply(errors.Wrap(err, "insertCapture"))
	}

	return newReply(nil)
}

// the reason that i'm implementing getCaptures to manually iterate on
// all tables and not to use a dbfunction that via dynamic sql would return
// the result, is that i am worried that someone might request too many captures
// and i would like the return to be streamed. therefore i iterate on the table and
// create a common replies. it also differs in the sense that it has a channel reply
func getCaptures(ex SessionExecutor, msg CommonMessage) chan CommonReply {
	retc := make(chan CommonReply)
	go func(SessionExecutor, CommonMessage, chan CommonReply) {
		defer close(retc)
		var tablename string
		cMsg := msg.(getCapMessage)
		getCapTablesTmpl := ex.getQuery(getCaptureTablesOp)
		stmt := fmt.Sprintf(getCapTablesTmpl, msg.GetMainTable())
		stime, etime := cMsg.getDates()
		rows, err := ex.Query(stmt, cMsg.getTableCol(), stime, etime)
		if err != nil {
			dbLogger.Errorf("getTable query error:%s", err)
			retc <- newGetCapReply(nil, err)
			return
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&tablename)
			if err != nil {
				dbLogger.Errorf("getCaptures can't find tables to scan:%s", err)
				retc <- newGetCapReply(nil, err)
				return
			}
			dbLogger.Infof("opening table:%s to get captures", tablename)
			//XXX implement

		}
	}(ex, msg, retc)
	return retc
}

func getCaptureBinaryStream(ctx context.Context, ex SessionExecutor, msg CommonMessage) chan CommonReply {
	// This has a buffer length of 1 so it can be cancelled and not block while
	// waiting to deliver the cancel message
	retc := make(chan CommonReply, 1)

	go func(ctx context.Context, ex SessionExecutor, msg CommonMessage, repStream chan CommonReply) {
		defer close(repStream)

		cMsg := msg.(getCapMessage)
		start, end := cMsg.getDates()
		tables, err := getCaptureTables(ex, cMsg.GetMainTable(), cMsg.getTableCol(), start, end)
		if err != nil {
			repStream <- newReply(err)
			return
		}

		selectCapTmpl := ex.getQuery(getCaptureBinaryOp)
		for _, tName := range tables {
			stmt := fmt.Sprintf(selectCapTmpl, tName)
			rows, err := ex.Query(stmt)
			if err != nil {
				repStream <- newReply(err)
				return
			}
			// Rows.close is not deferred here because we may be opening a lot of them.
			// Instead of waiting until the end of the function to close them, they are
			// closed after the loop, or within the cancellation case.
			for rows.Next() {
				cap := &Capture{fromTable: tName}
				err = rows.Scan(&cap.id, &cap.origin, &cap.protoMsg)

				select {
				case <-ctx.Done():
					repStream <- newReply(fmt.Errorf("context closed"))
					rows.Close()
					return
				case repStream <- newGetCapReply(cap, err):
					break
				}
			}
			rows.Close()
		}
	}(ctx, ex, msg, retc)

	return retc
}

func getPrefixStream(ctx context.Context, ex SessionExecutor, msg CommonMessage) chan CommonReply {
	retc := make(chan CommonReply, 1)

	go func(ctx context.Context, ex SessionExecutor, msg CommonMessage, repStream chan CommonReply) {
		defer close(repStream)

		cMsg := msg.(getCapMessage)
		start, end := cMsg.getDates()
		tables, err := getCaptureTables(ex, cMsg.GetMainTable(), cMsg.getTableCol(), start, end)
		if err != nil {
			repStream <- newReply(err)
		}

		selectPrefixTmpl := ex.getQuery(getPrefixOp)
		for _, tName := range tables {
			stmt := fmt.Sprintf(selectPrefixTmpl, tName)
			rows, err := ex.Query(stmt)
			if err != nil {
				repStream <- newReply(err)
				return
			}

			for rows.Next() {
				pref := ""
				err = rows.Scan(&pref)
				select {
				case <-ctx.Done():
					repStream <- newReply(fmt.Errorf("context closed"))
					rows.Close()
					return
				case repStream <- newGetPrefixReply(pref, err):
					break
				}
			}
			rows.Close()
		}
	}(ctx, ex, msg, retc)

	return retc
}

func getCaptureTables(ex SessionExecutor, dbTable, colName string, start, end time.Time) ([]string, error) {
	stmtTmpl := ex.getQuery(getCaptureTablesOp)
	stmt := fmt.Sprintf(stmtTmpl, dbTable, colName, start.Local().Format("2006-01-02 15:04:05"), end.Local().Format("2006-01-02 15:04:05"))

	tableNames := []string{}
	rows, err := ex.Query(stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		tName := ""
		err = rows.Scan(&tName)
		if err != nil {
			return nil, err
		}
		tableNames = append(tableNames, tName)
	}
	return tableNames, nil
}
