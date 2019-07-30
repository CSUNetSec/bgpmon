package db

import (
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/CSUNetSec/bgpmon/config"
	"github.com/CSUNetSec/bgpmon/util"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
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
	ID         string // the capture_id that together with the table makes it unique
	Timestamp  time.Time
	Origin     int // origin as
	Advertised []*net.IPNet
	Withdrawn  []*net.IPNet
	ASPath     []int
	ColIP      net.IP
	PeerIP     net.IP
	NextHop    net.IP
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

	err := rows.Scan(&c.ID, &c.Timestamp, &colIP, &peerIP, &asPath, &nextHop, &c.Origin, &advertized, &withdrawn)
	if err != nil {
		return err
	}

	if colIP.Valid {
		c.ColIP = net.ParseIP(colIP.String)
	} else {
		c.ColIP = nil
	}

	if peerIP.Valid {
		c.PeerIP = net.ParseIP(peerIP.String)
	} else {
		c.PeerIP = nil
	}

	if nextHop.Valid {
		c.NextHop = net.ParseIP(nextHop.String)
	} else {
		c.NextHop = nil
	}

	if asPath.Valid {
		c.ASPath, err = parseIntArray(asPath.String)
		if err != nil {
			return err
		}
	} else {
		c.ASPath = nil
	}

	if advertized.Valid {
		c.Advertised, err = parsePrefixArray(advertized.String)
		if err != nil {
			return err
		}
	} else {
		c.Advertised = nil
	}

	if withdrawn.Valid {
		c.Withdrawn, err = parsePrefixArray(withdrawn.String)
		if err != nil {
			return err
		}
	} else {
		c.Withdrawn = nil
	}

	return nil
}

// Values supplies values to a SQLExecutor
func (c *Capture) Values() []interface{} {
	ret := make([]interface{}, 8)

	ret[0] = c.Timestamp
	ret[1] = c.ColIP.String()
	ret[2] = c.PeerIP.String()
	ret[3] = pq.Array(c.ASPath)
	ret[4] = c.NextHop.String()
	ret[5] = c.Origin

	advArr := util.PrefixesToPQArray(c.Advertised)
	wdrArr := util.PrefixesToPQArray(c.Withdrawn)
	ret[6] = advArr
	ret[7] = wdrArr

	return ret
}

// NewCaptureFromPB returns a *Capture populated from a pb.BGPCapture
func NewCaptureFromPB(pbCap *pb.BGPCapture) (*Capture, error) {
	cap := &Capture{fromTable: "", ID: ""}
	var err error

	cap.Timestamp, cap.ColIP, err = util.GetTimeColIP(pbCap)
	if err != nil {
		return nil, dbLogger.Errorf("unable to parse collector IP: %s", err)
	}

	cap.PeerIP, err = util.GetPeerIP(pbCap)
	if err != nil {
		return nil, dbLogger.Errorf("unable to parse peer IP: %s", err)
	}

	// Ignoring the error here as this message could only have withdraws.
	cap.ASPath, _ = util.GetASPath(pbCap)

	cap.Origin = 0
	if len(cap.ASPath) != 0 {
		cap.Origin = cap.ASPath[len(cap.ASPath)-1]
	}

	cap.NextHop, err = util.GetNextHop(pbCap)
	if err != nil {
		cap.NextHop = net.IPv4(0, 0, 0, 0)
	}

	// Here if it errors and the return is nil, PrefixToPQArray should leave it and the schema should insert the default
	cap.Advertised, _ = util.GetAdvertisedPrefixes(pbCap)
	cap.Withdrawn, _ = util.GetWithdrawnPrefixes(pbCap)

	return cap, nil
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
	Name          string
	Email         string
	OwnedOrigins  []int
	OwnedPrefixes []*net.IPNet
}

// Values returns an array of interfaces that can be passed to a SQLExecutor
// to insert this Entity
func (e *Entity) Values() []interface{} {
	pqPrefs := util.PrefixesToPQArray(e.OwnedPrefixes)
	vals := make([]interface{}, 4)
	vals[0] = util.SanitizeDBString(e.Name)
	vals[1] = util.SanitizeDBString(e.Email)
	vals[2] = pq.Array(e.OwnedOrigins)
	vals[3] = pqPrefs

	return vals
}

// Scan populates this entity from a sql.Rows
func (e *Entity) Scan(rows *sql.Rows) error {
	var originsStr sql.NullString
	var prefixStr sql.NullString

	err := rows.Scan(&e.Name, &e.Email, &originsStr, &prefixStr)
	if err != nil {
		return err
	}

	if originsStr.Valid {
		e.OwnedOrigins, err = parseIntArray(originsStr.String)
		if err != nil {
			return err
		}
	} else {
		e.OwnedOrigins = nil
	}

	if prefixStr.Valid {
		e.OwnedPrefixes, err = parsePrefixArray(prefixStr.String)
		if err != nil {
			return err
		}
	} else {
		e.OwnedPrefixes = nil
	}

	return nil
}

// ToProtobuf returns a protobuf Entity with the same
// values as this entity
func (e *Entity) ToProtobuf() *pb.Entity {
	pbEnt := &pb.Entity{}
	pbEnt.Name = e.Name
	pbEnt.Email = e.Email

	pbEnt.OwnedOrigins = make([]int32, len(e.OwnedOrigins))
	for i, v := range e.OwnedOrigins {
		pbEnt.OwnedOrigins[i] = int32(v)
	}

	pbEnt.OwnedPrefixes = util.GetIPNetsAsPrefixList(e.OwnedPrefixes)

	return pbEnt
}

// NewEntityFromConfig returns an Entity populated from an EntityConfig.
func NewEntityFromConfig(ec *config.EntityConfig) (e *Entity, err error) {
	e = &Entity{}
	e.Name = ec.Name
	e.Email = ec.Email
	e.OwnedOrigins = ec.OwnedOrigins

	e.OwnedPrefixes = make([]*net.IPNet, len(ec.OwnedPrefixes))
	for i := range ec.OwnedPrefixes {
		_, e.OwnedPrefixes[i], err = net.ParseCIDR(ec.OwnedPrefixes[i])
		if err != nil {
			return nil, err
		}
	}

	return
}

// NewEntityFromPB returns an Entity populated from a protobuf.
func NewEntityFromPB(pbEnt *pb.Entity) (e *Entity, err error) {
	if pbEnt == nil {
		return nil, fmt.Errorf("nil pb.Entity")
	}
	e = &Entity{}
	e.Name = pbEnt.Name
	e.Email = pbEnt.Email

	e.OwnedOrigins = make([]int, len(pbEnt.OwnedOrigins))
	for i, v := range pbEnt.OwnedOrigins {
		e.OwnedOrigins[i] = int(v)
	}

	e.OwnedPrefixes, err = util.GetPrefixListAsIPNet(pbEnt.OwnedPrefixes)
	if err != nil {
		return nil, err
	}

	return e, nil
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
