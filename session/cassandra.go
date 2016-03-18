package session

import (
	"errors"
	"fmt"

	pb "github.com/CSUNetSec/bgpmon/protobuf"

	"github.com/gocql/gocql"
)

type CassandraConfig struct {
	Writers map[string][]WriterConfig
}

type WriterConfig struct {
	Keyspace string
}

type CassandraSession struct {
	Session
	cqlSession *gocql.Session
}

func NewCassandraSession(username, password string, hosts []string, config CassandraConfig) (Sessioner, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.LocalOne
	cluster.ProtoVersion = 4
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{10}
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password}
	cluster.NumConns = 16

	cqlSession, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	writers := make(map[pb.WriteRequest_Type][]Writer)
	for key, values := range config.Writers {
		for _, value := range values {
			switch key {
			case "BGPUpdateMsgByTime":
				addWriter(writers, pb.WriteRequest_BGP_UPDATE, BGPUpdateMsgByTime{CassandraWriter{cqlSession,value.Keyspace}})
			case "BGPUpdateMsgByPrefixRange":
				addWriter(writers, pb.WriteRequest_BGP_UPDATE, BGPUpdateMsgByPrefixRange{CassandraWriter{cqlSession,value.Keyspace}})
			case "LocationByAS":
				addWriter(writers, pb.WriteRequest_AS_NUMBER_LOCATION, LocationByAS{CassandraWriter{cqlSession,value.Keyspace}})
			case "LocationByIPAddress":
				addWriter(writers, pb.WriteRequest_IP_ADDRESS_LOCATION, LocationByIPAddress{CassandraWriter{cqlSession,value.Keyspace}})
			case "LocationByPrefix":
				addWriter(writers, pb.WriteRequest_PREFIX_LOCATION, LocationByPrefix{CassandraWriter{cqlSession,value.Keyspace}})
			default:
				return nil, errors.New(fmt.Sprintf("Unknown writer type %s for cassandra session", key))
			}
		}
	}

	cassSession := CassandraSession{Session{writers}, cqlSession}
	return cassSession, nil
}

func (c CassandraSession) Close() error {
	c.cqlSession.Close()
	return nil
}

func addWriter(writers map[pb.WriteRequest_Type][]Writer, writeRequestType pb.WriteRequest_Type, writer Writer) {
	if _, exists := writers[writeRequestType]; !exists {
		writers[writeRequestType] = []Writer{}
	}

	writers[writeRequestType] = append(writers[writeRequestType], writer)
}

/*
 * Writers
 */

const (
	locationByASStmt = "INSERT INTO %s.location_by_as_number(measure_date, as_number, country_code, state_code, city, latitude, longitude, source) VALUES(?,?,?,?,?,?,?,?)"
	locationByIPAddressStmt = "INSERT INTO %s.location_by_ip_address(measure_date, ip_address, country_code, state_code, city, latitude, longitude, source) VALUES(?,?,?,?,?,?,?,?)"
	locationByPrefixStmt = "INSERT INTO %s.location_by_as_number(measure_date, prefix_ip_address, prefix_mask, country_code, state_code, city, latitude, longitude, source) VALUES(?,?,?,?,?,?,?,?,?)"
)

type CassandraWriter struct {
	cqlSession *gocql.Session
	keyspace   string
}

type BGPUpdateMsgByTime struct {
	CassandraWriter
}

func (b BGPUpdateMsgByTime) Write(request *pb.WriteRequest) error {
	return errors.New("unimplented")
}

type BGPUpdateMsgByPrefixRange struct {
	CassandraWriter
}

func (b BGPUpdateMsgByPrefixRange) Write(request *pb.WriteRequest) error {
	return errors.New("unimplented")
}

type LocationByAS struct {
	CassandraWriter
}

func (l LocationByAS) Write(request *pb.WriteRequest) error {
	msg := request.GetAsNumberLocation()
	location := msg.GetLocation()
	return l.cqlSession.Query(
			fmt.Sprintf(locationByASStmt, l.keyspace),
			msg.AsNumber,
			msg.MeasureDate,
			location.CountryCode,
			location.StateCode,
			location.City,
			location.Latitude,
			location.Longitude,
			msg.Source,
		).Exec()
}

type LocationByIPAddress struct {
	CassandraWriter
}

func (l LocationByIPAddress) Write(request *pb.WriteRequest) error {
	msg := request.GetIpAddressLocation()
	location := msg.GetLocation()
	return l.cqlSession.Query(
			fmt.Sprintf(locationByIPAddressStmt, l.keyspace),
			msg.IpAddress,
			msg.MeasureDate,
			location.CountryCode,
			location.StateCode,
			location.City,
			location.Latitude,
			location.Longitude,
			msg.Source,
		).Exec()
}

type LocationByPrefix struct {
	CassandraWriter
}

func (l LocationByPrefix) Write(request *pb.WriteRequest) error {
	msg := request.GetPrefixLocation()
	location := msg.GetLocation()
	return l.cqlSession.Query(
			fmt.Sprintf(locationByPrefixStmt, l.keyspace),
			msg.PrefixIpAddress,
			msg.PrefixMask,
			msg.MeasureDate,
			location.CountryCode,
			location.StateCode,
			location.City,
			location.Latitude,
			location.Longitude,
			msg.Source,
		).Exec()
}
