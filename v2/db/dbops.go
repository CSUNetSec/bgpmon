package db

import (
	"fmt"
	"github.com/CSUNetSec/bgpmon/v2/config"
	"github.com/CSUNetSec/bgpmon/v2/util"
	"github.com/pkg/errors"
)

// DB Operations
// these require the sql executor to be already set up for them.
// checkschema makes sure that all the required tables exist in the database
func checkSchema(ex SessionExecutor, args sqlIn) (ret sqlOut) {
	csquery := ex.getdbop("checkschema")
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
	selectNodeTmpl := ex.getdbop("selectNodeTmpl")
	insertNodeTmpl := ex.getdbop("insertNodeTmpl")
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
			dblogger.Errorf("failed to insert config node. %s", err)
		} else {
			dblogger.Infof("inserted config node. %v", v)
		}
	}
	ret.knownNodes = allNodes
	return
}

// creates the necessary bgpmon schema, if the tables don't exist
func makeSchema(ex SessionExecutor, args sqlIn) (ret sqlOut) {
	maintableTmpl := ex.getdbop("makeMainTableTmpl")
	nodetableTmpl := ex.getdbop("makeNodeTableTmpl")
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
