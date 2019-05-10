package db

import (
	"database/sql"
	"net"
	"time"

	"github.com/CSUNetSec/bgpmon/config"
	"github.com/CSUNetSec/bgpmon/util"

	"github.com/lib/pq"
)

// node is a representation of a machine that is used as a BGP vantage point. It can be either
// a collector or a peer. If it is a collector, it is used to generate table names for captures
// seen by that collector. It can also be geolocated. Nodes can be discovered from stored messages
// or provided from a configuration file.
type node struct {
	name        string
	ip          string
	isCollector bool
	duration    int
	description string
	coords      string
	address     string
}

// newNode creates an empty node.
func newNode() *node {
	return &node{}
}

// nodeConfigFromNode creates a node configuration from a node.
func (n *node) nodeConfigFromNode() config.NodeConfig {
	return config.NodeConfig{
		Name:                n.name,
		IP:                  n.ip,
		IsCollector:         n.isCollector,
		DumpDurationMinutes: n.duration,
		Description:         n.description,
		Coords:              n.coords,
		Location:            n.address,
	}
}

// Capture represent a BGPCapture a row in the generated capture tables. It
// describes a single BGP event.
type Capture struct {
	fromTable string // mostly debug
	id        string // the capture_id that together with the table makes it unique
	timestamp time.Time
	origin    int // origin as
}

// Scan populates this capture with data from rows.Scan
func (c *Capture) Scan(rows *sql.Rows) error {
	// These should be replaced with appropriate fields and types inside
	// a capture.
	var (
		colIP      sql.NullString
		peerIP     sql.NullString
		asPath     sql.NullString
		nextHop    sql.NullString
		advertized sql.NullString
		withdrawn  sql.NullString
	)

	return rows.Scan(&c.id, &c.timestamp, &colIP, &peerIP, &asPath, &nextHop, &c.origin, &advertized, &withdrawn)
}

// CaptureTable represents a row in the main table. It describes
// an existing table populated with BGPCaptures
type CaptureTable struct {
	name      string
	collector string
	span      util.Timespan
}

// Entity represents a row in the entities table. It describes a party interested
// in particular BGP data, like the owner of a prefix.
type Entity struct {
	name          string
	email         string
	ownedOrigins  []int
	ownedPrefixes []*net.IPNet
}

// Values returns an array of interfaces that can be passed to a SQLExecutor
// to insert this Entity
func (e *Entity) Values() []interface{} {
	pqPrefs := util.PrefixesToPQArray(e.ownedPrefixes)
	vals := make([]interface{}, 4)
	vals[0] = e.name
	vals[1] = e.email
	vals[2] = pq.Array(e.ownedOrigins)
	vals[3] = pqPrefs

	return vals
}

// Scan populates this entity from a sql.Rows
func (e *Entity) Scan(rows *sql.Rows) error {
	name := ""
	email := ""
	var originsStr sql.NullString
	var prefixStr sql.NullString

	err := rows.Scan(&name, &email, &originsStr, &prefixStr)

	e.name = name
	e.email = email

	if originsStr.Valid {

	} else {
		e.ownedOrigins = nil
	}

	if prefixStr.Valid {

	} else {
		e.ownedPrefixes = nil
	}

	return err
}
