package location

import (
	"time"

	pb "github.com/CSUNetSec/bgpmon/protobuf"

	"github.com/gocql/gocql"
)

const (
	asNumberStmt  = "SELECT country_code, state_code, city, latitude, longitude, measure_date FROM csu_location_core.location_by_as_number WHERE as_number=?"
	ipAddressStmt = "SELECT country_code, state_code, city, latitude, longitude, measure_date FROM csu_location_core.location_by_ip_address WHERE ip_address=?"
	prefixStmt    = "SELECT country_code, state_code, city, latitude, longitude, measure_date FROM csu_location_core.location_by_prefix WHERE prefix_ip_address=? AND prefix_mask=?"
)

func GetLocationByASNumber(asNumber int, session *gocql.Session) []*pb.Location {
	query := session.Query(asNumberStmt, asNumber)
	return executeQuery(query)
}

func GetLocationByIPAddress(ipAddress string, session *gocql.Session) []*pb.Location {
	query := session.Query(ipAddressStmt, ipAddress)
	return executeQuery(query)
}

func GetLocationByPrefix(prefixIPAddress string, prefixPort int, session *gocql.Session) []*pb.Location {
	query := session.Query(prefixStmt, prefixIPAddress, prefixPort)
	return executeQuery(query)
}

func executeQuery(query *gocql.Query) (locations []*pb.Location) {
	var countryCode, stateCode, city string
	var latitude, longitude float64
	var date, currentDate time.Time

	iter := query.Iter()
	for iter.Scan(&countryCode, &stateCode, &city, &latitude, &longitude, &date) {
		if len(locations) == 0 {
			currentDate = date
		} else if currentDate != date {
			break
		}

		locations = append(locations, &pb.Location{countryCode, stateCode, city, latitude, longitude})
	}

	return
}
