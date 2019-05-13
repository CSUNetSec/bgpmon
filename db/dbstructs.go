package db

import (
	"database/sql"
	"net"
	"strconv"
	"strings"
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
	fromTable  string // mostly debug
	id         string // the capture_id that together with the table makes it unique
	timestamp  time.Time
	origin     int // origin as
	advertized []*net.IPNet
	withdrawn  []*net.IPNet
	asPath     []int
	colIP      net.IP
	peerIP     net.IP
	nextHop    net.IP
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

	err := rows.Scan(&c.id, &c.timestamp, &colIP, &peerIP, &asPath, &nextHop, &c.origin, &advertized, &withdrawn)
	if err != nil {
		return err
	}

	if colIP.Valid {
		c.colIP = net.ParseIP(colIP.String)
	} else {
		c.colIP = nil
	}

	if peerIP.Valid {
		c.peerIP = net.ParseIP(peerIP.String)
	} else {
		c.peerIP = nil
	}

	if nextHop.Valid {
		c.nextHop = net.ParseIP(nextHop.String)
	} else {
		c.nextHop = nil
	}

	if asPath.Valid {
		c.asPath, err = parseIntArray(asPath.String)
		if err != nil {
			return err
		}
	} else {
		c.asPath = nil
	}

	if advertized.Valid {
		c.advertized, err = parsePrefixArray(advertized.String)
		if err != nil {
			return err
		}
	} else {
		c.advertized = nil
	}

	if withdrawn.Valid {
		c.withdrawn, err = parsePrefixArray(withdrawn.String)
		if err != nil {
			return err
		}
	} else {
		c.withdrawn = nil
	}

	return nil
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
	var originsStr sql.NullString
	var prefixStr sql.NullString

	err := rows.Scan(&e.name, &e.email, &originsStr, &prefixStr)
	if err != nil {
		return err
	}

	if originsStr.Valid {
		e.ownedOrigins, err = parseIntArray(originsStr.String)
		if err != nil {
			return err
		}
	} else {
		e.ownedOrigins = nil
	}

	if prefixStr.Valid {
		e.ownedPrefixes, err = parsePrefixArray(prefixStr.String)
		if err != nil {
			return err
		}
	} else {
		e.ownedPrefixes = nil
	}

	return nil
}

// parseIntArray takes a DB array string and returns an int array from that.
// This is convenient since multiple structs might need an AS path or array
// that needs to be parsed from the DB.
func parseIntArray(arr string) ([]int, error) {
	var ret []int

	asArr := parseDBArray(arr)
	if asArr == nil {
		return nil, nil
	}

	for _, v := range asArr {
		as, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return nil, err
		}

		ret = append(ret, int(as))
	}

	return ret, nil
}

func parsePrefixArray(arr string) ([]*net.IPNet, error) {
	if arr == "" {
		return nil, nil
	}

	var ret []*net.IPNet

	prefArr := parseDBArray(arr)
	if prefArr == nil {
		return nil, nil
	}

	for _, v := range prefArr {
		_, ipNet, err := net.ParseCIDR(v)
		if err != nil {
			return nil, err
		}
		ret = append(ret, ipNet)
	}

	return ret, nil
}

// parseDBArray turns strings of the format {a,b,c} into
// an array of strings, {"a", "b", "c"}
func parseDBArray(arr string) []string {
	if arr == "{}" {
		return nil
	}
	elementStr := arr[1 : len(arr)-1]
	return strings.Split(elementStr, ",")
}
