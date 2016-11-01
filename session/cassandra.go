package session

import (
	"errors"
	"fmt"
	"net"
	"time"

	pb "github.com/CSUNetSec/bgpmon/pb"

	"github.com/gocql/gocql"
	//"github.com/golang/protobuf/proto"
)

const (
	writeretries = 3
)

type CassandraConfig struct {
	Writers map[string][]WriterConfig
}

type WriterConfig struct {
	Keyspace          string
	TimeBucketSeconds int64
}

type CassandraSession struct {
	*Session
	CqlSession *gocql.Session
}

func NewCassandraSession(username, password string, hosts []string, workerCount uint32, config CassandraConfig) (Sessioner, error) {
	cluster := gocql.NewCluster(hosts...)
	//cluster.Consistency = gocql.LocalOne
	cluster.ProtoVersion = 3
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{10}
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password}
	cluster.NumConns = 16

	cqlSession, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	writers := make(map[pb.WriteRequest_Type][]Writer)
	for writerType, writerConfigs := range config.Writers {
		for _, writerConfig := range writerConfigs {
			switch writerType {
			case "BGPUpdateMsgByTime":
				addWriter(writers, pb.WriteRequest_BGP_UPDATE, BGPUpdateMsgByTime{CassandraWriter{cqlSession, writerConfig.Keyspace}, writerConfig.TimeBucketSeconds})
			case "BGPUpdateMsgByPrefixRange":
				addWriter(writers, pb.WriteRequest_BGP_UPDATE, BGPUpdateMsgByPrefixRange{CassandraWriter{cqlSession, writerConfig.Keyspace}, writerConfig.TimeBucketSeconds})
			case "LocationByAS":
				addWriter(writers, pb.WriteRequest_AS_NUMBER_LOCATION, LocationByAS{CassandraWriter{cqlSession, writerConfig.Keyspace}})
			case "LocationByIPAddress":
				addWriter(writers, pb.WriteRequest_IP_ADDRESS_LOCATION, LocationByIPAddress{CassandraWriter{cqlSession, writerConfig.Keyspace}})
			case "LocationByPrefix":
				addWriter(writers, pb.WriteRequest_PREFIX_LOCATION, LocationByPrefix{CassandraWriter{cqlSession, writerConfig.Keyspace}})
			default:
				return nil, errors.New(fmt.Sprintf("Unknown writer type %s for cassandra session", writerType))
			}
		}
	}

	session, err := NewSession(writers, workerCount)
	if err != nil {
		return nil, err
	}

	return CassandraSession{&session, cqlSession}, nil
}

func (c CassandraSession) Close() error {
	c.CqlSession.Close()
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
	bgpUpdateMsgByTimeStmt        = "INSERT INTO %s.update_messages_by_time(time_bucket, timestamp, advertised_prefixes, as_path, collector_ip_address, collector_mac_address, collector_port, next_hop, peer_ip_address, withdrawn_prefixes) VALUES(?,?,?,?,?,?,?,?,?,?)"
	bgpUpdateMsgByPrefixRangeStmt = "INSERT INTO %s.as_number_by_prefix_range(time_bucket, prefix_ip_address, prefix_mask, timestamp, as_number) VALUES(?,?,?,?,?)"
	locationByASStmt              = "INSERT INTO %s.location_by_as_number(as_number, measure_date, country_code, state_code, city, latitude, longitude, source) VALUES(?,?,?,?,?,?,?,?)"
	locationByIPAddressStmt       = "INSERT INTO %s.location_by_ip_address(ip_address, measure_date, country_code, state_code, city, latitude, longitude, source) VALUES(?,?,?,?,?,?,?,?)"
	locationByPrefixStmt          = "INSERT INTO %s.location_by_prefix(prefix_ip_address, prefix_mask, measure_date, country_code, state_code, city, latitude, longitude, source) VALUES(?,?,?,?,?,?,?,?,?)"
)

type CassandraWriter struct {
	cqlSession *gocql.Session
	keyspace   string
}

type BGPUpdateMsgByTime struct {
	CassandraWriter
	timeBucketSeconds int64
}

type IPPrefix struct {
	IP   net.IP `cql:"ip_address"`
	Mask uint8  `cql:"mask"`
}

func (b BGPUpdateMsgByTime) Write(request *pb.WriteRequest) error {
	//get message and convert timestamp to timeuuid
	errcount := 0
	msg := request.GetBgpUpdateMessage()
	timestamp := gocql.UUIDFromTime(time.Unix(int64(msg.Timestamp), 0))

	advertisedPrefixes := []IPPrefix{}
	for _, ipPrefix := range msg.AdvertisedPrefixes {
		advertisedPrefixes = append(advertisedPrefixes, IPPrefix{net.ParseIP(ipPrefix.PrefixIpAddress), uint8(ipPrefix.PrefixMask)})
	}

	withdrawnPrefixes := []IPPrefix{}
	for _, ipPrefix := range msg.WithdrawnPrefixes {
		withdrawnPrefixes = append(withdrawnPrefixes, IPPrefix{net.ParseIP(ipPrefix.PrefixIpAddress), uint8(ipPrefix.PrefixMask)})
	}
	//fmt.Printf("by time msg with timestamp :%s\n", timestamp)

retry:
	if errcount < writeretries {

		err := b.cqlSession.Query(
			fmt.Sprintf(bgpUpdateMsgByTimeStmt, b.keyspace),
			time.Unix(msg.Timestamp-(msg.Timestamp%b.timeBucketSeconds), 0),
			timestamp,
			advertisedPrefixes,                  //announced_prefixes
			msg.AsPath,                          //as_path
			net.ParseIP(msg.CollectorIpAddress), //collector_ip_address
			msg.CollectorMacAddress,             //collector_mac_address
			msg.CollectorPort,                   //collector_port
			nil,                                 //TODO msg.NextHop next_hop
			net.ParseIP(msg.PeerIpAddress), //peer_ip_address
			withdrawnPrefixes,              //withdrawn_prefixes
		).Exec()

		if err != nil {
			fmt.Printf("aprefix:%v\naspath:%v\ncip:%v\ncmac:%v\ncport:%v\nnhop:%v\npip:%v\nwprefix:%v\n--------------------------\n",
				advertisedPrefixes,
				msg.AsPath,
				net.ParseIP(msg.CollectorIpAddress),
				msg.CollectorMacAddress,
				msg.CollectorPort,
				msg.NextHop,
				net.ParseIP(msg.PeerIpAddress),
				withdrawnPrefixes)
			errcount++
			goto retry
		}
	}

	return nil
}

type BGPUpdateMsgByPrefixRange struct {
	CassandraWriter
	timeBucketSeconds int64
}

func (b BGPUpdateMsgByPrefixRange) Write(request *pb.WriteRequest) error {
	errcount := 0
	var err error
	//get message and convert timestamp to timeuuid
	msg := request.GetBgpUpdateMessage()
	timestamp := gocql.UUIDFromTime(time.Unix(int64(msg.Timestamp), 0))
	//fmt.Printf("by prefix msg with timestamp :%s\n", timestamp)

	for _, prefix := range msg.AdvertisedPrefixes {
		//parse ip address
		prefixIP := net.ParseIP(prefix.PrefixIpAddress)

	retry:
		if errcount < writeretries {
			err = b.cqlSession.Query(
				fmt.Sprintf(bgpUpdateMsgByPrefixRangeStmt, b.keyspace),
				time.Unix(msg.Timestamp-(msg.Timestamp%b.timeBucketSeconds), 0),
				prefixIP,
				prefix.PrefixMask,
				timestamp,
				msg.AsPath[len(msg.AsPath)-1],
			).Exec()

			if err != nil {
				errcount++
				goto retry
			}
		} else {
			return err
		}
	}

	return nil
}

type LocationByAS struct {
	CassandraWriter
}

func (l LocationByAS) Write(request *pb.WriteRequest) error {
	msg := request.GetAsNumberLocation()
	location := msg.GetLocation()
	measureDate, err := time.Parse("2006-01-02", msg.MeasureDate)
	if err != nil {
		return err
	}

	return l.cqlSession.Query(
		fmt.Sprintf(locationByASStmt, l.keyspace),
		msg.AsNumber,
		gocql.UUIDFromTime(measureDate),
		location.CountryCode,
		location.StateCode,
		location.City,
		float32(location.Latitude),
		float32(location.Longitude),
		msg.Source,
	).Exec()
}

type LocationByIPAddress struct {
	CassandraWriter
}

func (l LocationByIPAddress) Write(request *pb.WriteRequest) error {
	msg := request.GetIpAddressLocation()
	location := msg.GetLocation()
	measureDate, err := time.Parse("2006-01-02", msg.MeasureDate)
	if err != nil {
		return err
	}

	return l.cqlSession.Query(
		fmt.Sprintf(locationByIPAddressStmt, l.keyspace),
		msg.IpAddress,
		gocql.UUIDFromTime(measureDate),
		location.CountryCode,
		location.StateCode,
		location.City,
		float32(location.Latitude),
		float32(location.Longitude),
		msg.Source,
	).Exec()
}

type LocationByPrefix struct {
	CassandraWriter
}

func (l LocationByPrefix) Write(request *pb.WriteRequest) error {
	msg := request.GetPrefixLocation()
	location := msg.GetLocation()
	measureDate, err := time.Parse("2006-01-02", msg.MeasureDate)
	if err != nil {
		return err
	}

	return l.cqlSession.Query(
		fmt.Sprintf(locationByPrefixStmt, l.keyspace),
		msg.Prefix.PrefixIpAddress,
		msg.Prefix.PrefixMask,
		gocql.UUIDFromTime(measureDate),
		location.CountryCode,
		location.StateCode,
		location.City,
		float32(location.Latitude),
		float32(location.Longitude),
		msg.Source,
	).Exec()
}
