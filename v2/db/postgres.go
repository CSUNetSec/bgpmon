package db

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/CSUNetSec/bgpmon/v2/config"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

type postgresSession struct {
	db        *sql.DB
	dbname    string
	parentctx context.Context
	colstrs   collectorsByNameDate
}

//implement Dber
func (ps *postgresSession) Db() *sql.DB {
	return ps.db
}

func (ps *postgresSession) GetParentContext() context.Context {
	return ps.parentctx
}

//a wrapper struct that can contain all the possible arguments to our calls
type sqlIn struct {
	dbname     string //the name of the database we're operating on
	maintable  string //the table which references all collector-day tables.
	nodetable  string //the table with nodes and their configurations
	knownNodes map[string]config.NodeConfig
}

type sqlOut struct {
	ok         bool
	err        error
	knownNodes map[string]config.NodeConfig
}

type workFunc func(sqlCtxExecutor, sqlIn) sqlOut

func retCheckSchema(o sqlOut) (bool, error) {
	return o.ok, o.err
}

func checkSchema(ex sqlCtxExecutor, args sqlIn) (ret sqlOut) {
	const q = `
SELECT EXISTS (
   SELECT *
   FROM   information_schema.tables
   WHERE  table_name = $1
 );
`
	var (
		res bool
		err error
	)
	tocheck := []string{args.maintable, args.nodetable}
	allgood := true
	for _, tname := range tocheck {
		if err = ex.QueryRow(q, tname).Scan(&res); err != nil {
			ret.ok, ret.err = false, errors.Wrap(err, "checkSchema")
			return
		}
		dblogger.Infof("table:%s exists:%v", tname, res)
		allgood = allgood && res
	}
	ret.ok, ret.err = allgood, nil
	return
}

func retMakeSchema(o sqlOut) error {
	return o.err
}

func syncNodes(ex sqlCtxExecutor, args sqlIn) (ret sqlOut) {
	const selectNodeTmpl = `
		SELECT name, ip, isCollector, tableDumpDurationMinutes, description, coords, address FROM %s;
		`
	const insertNodeTmpl = `
		   INSERT INTO %s (name, ip, isCollector, tableDumpDurationMinutes, description, coords, address) VALUES ($1, $2, $3, $4, $5, $6, $7)
		   ON CONFLICT (ip) DO UPDATE SET name=EXCLUDED.name, isCollector=EXCLUDED.isCollector,tableDumpDurationMinutes=EXCLUDED.tableDumpDurationMinutes,
		   description=EXCLUDED.description, coords=EXCLUDED.coords, address=EXCLUDED.address;
		   `
	var (
		nodeName      string
		nodeIP        string
		nodeCollector bool
		nodeDuration  int
		nodeDescr     string
		nodeCoords    string
		nodeAddress   string
	)
	rows, err := ex.Query(fmt.Sprintf(selectNodeTmpl, args.nodetable))
	if err != nil {
		dblogger.Errorf("syncNode query:", err)
		ret.err = err
		return
	}
	dbNodes := make(map[string]config.NodeConfig)
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&nodeName, &nodeIP, &nodeCollector, &nodeDuration, &nodeDescr, &nodeCoords, &nodeAddress)
		if err != nil {
			dblogger.Errorf("syncnode fetch node row:%s", err)
			ret.err = err
			return
		}
		hereNewNode := config.NodeConfig{
			Name:                nodeName,
			IP:                  nodeIP,
			IsCollector:         nodeCollector,
			DumpDurationMinutes: nodeDuration,
			Description:         nodeDescr,
			Coords:              nodeCoords,
			Location:            nodeAddress,
		}
		dbNodes[hereNewNode.IP] = hereNewNode
	}
	allNodes := sumNodeConfs(args.knownNodes, dbNodes)
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
			dblogger.Errorf("failed to insert config node. %s", err)
		} else {
			dblogger.Infof("inserted config node. %v", v)
		}
	}
	ret.knownNodes = allNodes
	return
}

func sumNodeConfs(confnodes, dbnodes map[string]config.NodeConfig) map[string]config.NodeConfig {
	ret := make(map[string]config.NodeConfig)
	for k1, v1 := range confnodes {
		ret[k1] = v1 //if it exists in config, prefer config
	}
	for k2, v2 := range dbnodes {
		if _, ok := confnodes[k2]; ok { // exists in config, so ignore it
			continue
		}
		//does not exist in config, so add it in ret as it is in the db
		ret[k2] = v2
	}
	return ret
}

func makeSchema(ex sqlCtxExecutor, args sqlIn) (ret sqlOut) {
	const maintableTmpl = `
CREATE TABLE IF NOT EXISTS %s (
	dbname varchar PRIMARY KEY,
	collector varchar,
	dateFrom timestamp,
	dateTo timestamp
); 
`
	const nodetableTmpl = `
CREATE TABLE IF NOT EXISTS %s (
	ip varchar PRIMARY KEY,
	name varchar, 
	isCollector boolean,
	tableDumpDurationMinutes integer,
	description varchar,
	coords varchar,
	address varchar
); 
`
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

func (ps *postgresSession) Write(wr *pb.WriteRequest) error {
	dblogger.Infof("postgres write called with request:%s", wr)
	return nil
}

func (ps *postgresSession) Close() error {
	dblogger.Info("postgres close called")
	//ps.wm.Stop()
	if err := ps.db.Close(); err != nil {
		return errors.Wrap(err, "db close")
	}
	return nil
}

func newPostgresSession(ctx context.Context, conf config.SessionConfiger, id string, nw int) (*postgresSession, error) {
	var (
		db  *sql.DB
		err error
	)
	dblogger.Infof("postgres db session starting")
	u := conf.GetUser()
	p := conf.GetPassword()
	d := conf.GetDatabaseName()
	h := conf.GetHostNames()
	cd := conf.GetCertDir()
	if len(h) == 1 && p != "" && cd == "" && u != "" { //no ssl standard pw
		db, err = sql.Open("postgres", fmt.Sprintf("user=%s password=%s dbname=%s host=%s sslmode=disable", u, p, d, h[0]))
	} else if cd != "" && u != "" { //ssl
		db, err = sql.Open("postgres", fmt.Sprintf("user=%s password=%s dbname=%s host=%s", u, p, d, h[0]))
	} else {
		return nil, errors.New("Postgres sessions require a password and exactly one hostname")
	}
	if err != nil {
		return nil, errors.Wrap(err, "sql open")
	}
	//create the worker manager and give the session to it so it can access the db,
	//wm := NewWorkerMgr(nw, db, ctx)
	//create the session object.
	//ps := &postgresSession{dbname: d, parentctx: ctx, wm: wm}
	ps := &postgresSession{dbname: d, parentctx: ctx, db: db}
	return ps, nil
}

func (ps *postgresSession) schemaSyncNodes(known map[string]config.NodeConfig) (map[string]config.NodeConfig, error) {
	//ping to see we can connect to the db
	if err := ps.db.Ping(); err != nil {
		return nil, errors.Wrap(err, "sql ping")
	}
	if pctx, err := GetNewExecutor(ps.parentctx, ps, false, CTXTIMEOUT); err != nil {
		return nil, errors.Wrap(err, "newCtxTx")
	} else {
		snargs := sqlIn{dbname: "bgpmon", nodetable: "nodes", knownNodes: known}
		syncNodes(pctx, snargs)
	}
	return nil, nil
}

//Checks the state and initializes the appropriate schemas
func (ps *postgresSession) schemaCheckInit() error {
	//ping to see we can connect to the db

	if err := ps.db.Ping(); err != nil {
		return errors.Wrap(err, "sql ping")
	}
	//check for required main tables on the schema.
	if pctx, err := GetNewExecutor(ps.parentctx, ps, true, CTXTIMEOUT); err != nil {
		return errors.Wrap(err, "newCtxTx")
	} else {
		csargs := sqlIn{dbname: "bgpmon", maintable: "dbs", nodetable: "nodes"}
		if ok, err := retCheckSchema(checkSchema(pctx, csargs)); err != nil {
			return errors.Wrap(err, "retCheckschema")
		} else {
			if !ok { // table does not exist
				dblogger.Infof("creating schema tables")
				if err = retMakeSchema(makeSchema(pctx, csargs)); err != nil {
					return errors.Wrap(err, "retMakeSchema")
				}
				dblogger.Infof("all good. commiting the changes")
				if err = pctx.Done(); err != nil {
					return errors.Wrap(err, "makeschema commit")
				}
			} else {
				dblogger.Infof("all main bgpmon tables exist.")
			}
		}
	}
	return nil
}
