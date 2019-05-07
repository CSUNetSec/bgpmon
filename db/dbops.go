package db

import (
	"context"
	"database/sql"
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

// This is a utility function that can be deferred while
// not ignoring an error.
func closeRowsAndLog(rows *sql.Rows) {
	if err := rows.Close(); err != nil {
		dbLogger.Errorf("Error closing rows: %s", err)
	}
}

// DB Operations
// These functions require that a prepared SessionExecutor be passed to them.

// checkSchema makes sure that all the required tables exist in the database.
func checkSchema(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	csQuery := ex.getQuery(checkSchemaOp)

	toCheck := []string{msg.GetMainTable(), msg.GetNodeTable(), msg.GetEntityTable()}
	allGood := true
	for _, tName := range toCheck {
		res := false
		if err := ex.QueryRow(csQuery, tName).Scan(&res); err != nil {
			return newReply(errors.Wrap(err, "checkSchema"))
		}
		dbLogger.Infof("table:%s exists:%v", tName, res)
		allGood = allGood && res
	}

	if !allGood {
		return newReply(errNoTable)
	}

	return newReply(nil)
}

// syncNodes finds all the known nodes in the db, and composes them with the incoming nodes
// from the config then returns back the aggregate. If a node exists in both the config and
// the DB, the config is preferred.
func syncNodes(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	nodesMsg := msg.(nodesMessage)

	selectNodeTmpl := ex.getQuery(selectNodeOp)
	insertNodeTmpl := ex.getQuery(insertNodeOp)
	// This keeps nodes recovered from the DB.
	dbNodes := make(map[string]config.NodeConfig)
	// The current node we will be looping over.
	cn := newNode()
	rows, err := ex.Query(fmt.Sprintf(selectNodeTmpl, nodesMsg.GetNodeTable()))
	if err != nil {
		return newNodesReply(nil, dbLogger.Errorf("syncNode query: %v", err))
	}
	defer closeRowsAndLog(rows)

	for rows.Next() {
		err := rows.Scan(&cn.name, &cn.ip, &cn.isCollector, &cn.duration, &cn.description, &cn.coords, &cn.address)
		if err != nil {
			return newNodesReply(nil, dbLogger.Errorf("syncnode fetch node row:%s", err))
		}

		node := cn.nodeConfigFromNode()
		dbNodes[node.IP] = node
	}

	dbLogger.Infof("Calling sumnodes, Known: %v, DB: %v", nodesMsg.getNodes(), dbNodes)

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

// getNode returns the first matching node from the db table based on IP or name.
func getNode(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	nodeMsg := msg.(nodeMessage)

	selectNodeTmpl := ex.getQuery(selectNodeOp)
	cn := newNode()
	rows, err := ex.Query(fmt.Sprintf(selectNodeTmpl, nodeMsg.GetNodeTable()))
	if err != nil {
		dbLogger.Errorf("getNode query error:%s", err)
		return newNodeReply(nil, err)
	}
	defer closeRowsAndLog(rows)

	for rows.Next() {
		err := rows.Scan(&cn.name, &cn.ip, &cn.isCollector, &cn.duration, &cn.description, &cn.coords, &cn.address)
		if err != nil {
			dbLogger.Errorf("getNode fetch node row:%s", err)
			return newNodeReply(nil, err)
		}
		// Try to match the node.
		dbLogger.Infof("trying node matching with name:%s ip:%s", cn.name, cn.ip)
		name, ip := nodeMsg.getNodeName(), nodeMsg.getNodeIP()
		if (name == cn.name && name != "") || (ip == cn.ip && ip != "") {
			return newNodeReply(cn, nil)
		}
	}

	return newNodeReply(nil, errNoNode)
}

// createCaptureTable creates a table to hold captures and registers it in the main table and
// the current known tables in memory.
func createCaptureTable(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	createCapTmpl := ex.getQuery(makeCaptureTableOp)

	cMsg := msg.(capTableMessage)
	name := cMsg.getTableName()

	stmt := fmt.Sprintf(createCapTmpl, name)
	_, err := ex.Exec(stmt)
	if err != nil {
		return newCapTableReply("", "", time.Now(), time.Now(), dbLogger.Errorf("createCaptureTable error: %s", err))
	}

	insertCapTmpl := ex.getQuery(insertMainTableOp)
	// This returns the collector IP.
	ip := cMsg.getTableCol()
	start, end := cMsg.getDates()
	_, err = ex.Exec(fmt.Sprintf(insertCapTmpl, cMsg.GetMainTable()), name, ip, start, end)

	if err != nil {
		return newCapTableReply("", "", time.Now(), time.Now(), dbLogger.Errorf("createCaptureTable insertnode error:%s", err))
	}
	dbLogger.Infof("inserted table:%s", name)

	return newCapTableReply(name, ip, start, end, nil)
}

// getTable returns the collector table from the main dbs table.
func getTable(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	tMsg := msg.(tableMessage)

	selectTableTmpl := ex.getQuery(selectTableOp)
	colIP := tMsg.getColIP()
	// This cast to UTC is important, the DB doesn't do it automatically.
	qdate := tMsg.getDate().UTC()
	rows, err := ex.Query(fmt.Sprintf(selectTableTmpl, tMsg.GetMainTable(), tMsg.GetNodeTable()), qdate, colIP)
	if err != nil {
		return newTableReply("", time.Now(), time.Now(), nil, dbLogger.Errorf("getTable query error:%s", err))
	}
	defer closeRowsAndLog(rows)

	if rows.Next() {
		dbName := ""
		collector := ""
		var start time.Time
		var end time.Time
		duration := 0

		err := rows.Scan(&dbName, &collector, &start, &end, &duration)
		if err != nil {
			return newTableReply("", time.Now(), time.Now(), nil, dbLogger.Errorf("getNode fetch node row:%s", err))
		}
		// We found a table for that range.
		n := &node{ip: colIP, name: collector, duration: duration}
		return newTableReply(dbName, start, end, n, nil)
	}

	return newTableReply("", time.Now(), time.Now(), nil, errNoTable)
}

// makeSchema creates the necessary bgpmon schema if the tables don't exist.
func makeSchema(ex SessionExecutor, msg CommonMessage) (rep CommonReply) {
	mainTableTmpl := ex.getQuery(makeMainTableOp)
	nodeTableTmpl := ex.getQuery(makeNodeTableOp)
	entityTableTmpl := ex.getQuery(makeEntityTableOp)

	if _, err := ex.Exec(fmt.Sprintf(mainTableTmpl, msg.GetMainTable())); err != nil {
		return newReply(errors.Wrap(err, "makeSchema maintable"))
	}
	dbLogger.Infof("created table:%s", msg.GetMainTable())

	if _, err := ex.Exec(fmt.Sprintf(nodeTableTmpl, msg.GetNodeTable())); err != nil {
		return newReply(errors.Wrap(err, "makeSchema nodeTable"))
	}
	dbLogger.Infof("created table:%s", msg.GetNodeTable())

	if _, err := ex.Exec(fmt.Sprintf(entityTableTmpl, msg.GetEntityTable())); err != nil {
		return newReply(errors.Wrap(err, "makeSchema entityTable"))
	}
	dbLogger.Infof("created table:%s", msg.GetEntityTable())

	return newReply(nil)
}

// insertCapture inserts a capture onto the appropriate table.
// Currently unused.
func insertCapture(ex SessionExecutor, msg CommonMessage) CommonReply {
	cMsg := msg.(captureMessage)

	insertTmpl := ex.getQuery(insertCaptureTableOp)
	stmt := fmt.Sprintf(insertTmpl, cMsg.getTableName())

	advPrefixes, _ := util.GetAdvertisedPrefixes(cMsg.getCapture())
	wdrPrefixes, _ := util.GetWithdrawnPrefixes(cMsg.getCapture())

	adv := util.PrefixesToPQArray(advPrefixes)
	wdr := util.PrefixesToPQArray(wdrPrefixes)

	capTime, colIP, _ := util.GetTimeColIP(cMsg.getCapture())
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

	_, err := ex.Exec(stmt, capTime, colIP.String(), peerIP.String(), pq.Array(asPath), nextHop.String(), origin, adv, wdr, protoMsg)
	if err != nil {
		dbLogger.Infof("failing to insert capture: time:%s colip:%s  aspath:%v oas:%v ", capTime, colIP.String(), asPath, origin)
		return newReply(errors.Wrap(err, "insertCapture"))
	}

	return newReply(nil)
}

// getCaptures returns a stream of Captures
// Currently unused.
func getCaptures(ex SessionExecutor, msg CommonMessage) chan CommonReply {
	retC := make(chan CommonReply)
	go func(ex SessionExecutor, msg CommonMessage, retC chan CommonReply) {
		defer close(retC)
		getCapTablesTmpl := ex.getQuery(getCaptureTablesOp)

		cMsg := msg.(getCapMessage)
		stmt := fmt.Sprintf(getCapTablesTmpl, msg.GetMainTable())
		start, end := cMsg.getDates()

		rows, err := ex.Query(stmt, cMsg.getTableCol(), start, end)
		if err != nil {
			dbLogger.Errorf("getTable query error:%s", err)
			retC <- newGetCapReply(nil, err)
			return
		}
		defer closeRowsAndLog(rows)

		for rows.Next() {
			tableName := ""
			err := rows.Scan(&tableName)
			if err != nil {
				dbLogger.Errorf("getCaptures can't find tables to scan:%s", err)
				retC <- newGetCapReply(nil, err)
				return
			}
			dbLogger.Infof("opening table:%s to get captures", tableName)

		}
	}(ex, msg, retC)
	return retC
}

// getCaptureBinaryStream returns a stream of Captures
func getCaptureBinaryStream(ctx context.Context, ex SessionExecutor, msg CommonMessage) chan CommonReply {

	// This has a buffer length of 1 so it can be cancelled and not block while
	// waiting to deliver the cancel message
	retC := make(chan CommonReply, 1)

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

			// Rows.Close is not deferred here because we may be opening a lot of them.
			// Instead of waiting until the end of the function to close them, they are
			// closed after the loop, or within the cancellation case.
			for rows.Next() {
				cap := &Capture{fromTable: tName}
				err = rows.Scan(&cap.id, &cap.origin, &cap.protoMsg)

				select {
				case <-ctx.Done():
					repStream <- newReply(fmt.Errorf("context closed"))
					closeRowsAndLog(rows)
					return
				case repStream <- newGetCapReply(cap, err):
					break
				}
			}
			closeRowsAndLog(rows)
		}
	}(ctx, ex, msg, retC)

	return retC
}

// getPrefixStream returns a stream of prefixes.
func getPrefixStream(ctx context.Context, ex SessionExecutor, msg CommonMessage) chan CommonReply {
	retC := make(chan CommonReply, 1)

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
					closeRowsAndLog(rows)
					return
				case repStream <- newGetPrefixReply(pref, err):
					break
				}
			}
			closeRowsAndLog(rows)
		}
	}(ctx, ex, msg, retC)

	return retC
}

func getCaptureTables(ex SessionExecutor, dbTable, colName string, start, end time.Time) ([]string, error) {
	stmtTmpl := ex.getQuery(getCaptureTablesOp)
	timeFormat := "2006-01-02 15:04:05"
	stmt := fmt.Sprintf(stmtTmpl, dbTable, colName, start.Local().Format(timeFormat), end.Local().Format(timeFormat))

	var tableNames []string
	rows, err := ex.Query(stmt)
	if err != nil {
		return nil, err
	}
	defer closeRowsAndLog(rows)

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

func insertEntity(ex SessionExecutor, msg CommonMessage) CommonReply {
	stmtTmpl := ex.getQuery(insertEntityOp)
	stmt := fmt.Sprintf(stmtTmpl, msg.GetEntityTable())

	entMsg := msg.(*entityMessage)
	entity := entMsg.getEntity()

	_, err := ex.Exec(stmt, entity.Values()...)
	return newReply(err)
}
