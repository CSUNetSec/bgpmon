package session

import (
	"errors"
	"fmt"
	"net"
	"time"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon"
	pbcom "github.com/CSUNetSec/netsec-protobufs/common"
	pbbgp "github.com/CSUNetSec/netsec-protobufs/protocol/bgp"
	"github.com/golang/protobuf/proto"

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

func parseIp(a pbcom.IPAddressWrapper) (net.IP, error) {
	if len(a.Ipv6) != 0 {
		return net.IP(a.Ipv6), nil
	} else if len(a.Ipv4) != 0 {
		return net.IP(a.Ipv4).To4(), nil
	} else {
		return nil, fmt.Errorf("IP Prefix that is neither v4 or v6 found in BGPCapture msg")
	}
}

func getAsPath(a pbbgp.BGPUpdate) []uint32 {
	ret := []uint32{}
	if a.Attrs != nil {
		for _, seg := range a.Attrs.AsPath {
			if seg.AsSeq != nil {
				ret = append(ret, seg.AsSeq...)
			}
			if seg.AsSet != nil {
				ret = append(ret, seg.AsSet...)
			}

		}
	}
	return ret
}

func NewCassandraSession(username, password string, hosts []string, workerCount uint32, config CassandraConfig) (Sessioner, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.LocalOne
	cluster.ProtoVersion = 4
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{10}
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password}
	cluster.NumConns = 16
	//cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.Timeout = time.Duration(1200 * time.Millisecond)

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
			case "BGPCaptureByTime":
				addWriter(writers, pb.WriteRequest_BGP_CAPTURE, BGPCaptureByTime{CassandraWriter{cqlSession, writerConfig.Keyspace}, writerConfig.TimeBucketSeconds})
			case "BGPCaptureByPrefixRange":
				addWriter(writers, pb.WriteRequest_BGP_CAPTURE, BGPCaptureByPrefixRange{CassandraWriter{cqlSession, writerConfig.Keyspace}, writerConfig.TimeBucketSeconds})
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
	bgpCaptureByTimeStmt          = "INSERT INTO %s.update_messages_by_time(time_bucket, timestamp, advertised_prefixes, as_path, collector_ip_address, next_hop, peer_ip_address, withdrawn_prefixes, protomsg) VALUES(?,?,?,?,?,?,?,?,?)"
	bgpCaptureByPrefixRangeStmt   = "INSERT INTO %s.as_number_by_prefix_range(time_bucket, prefix_ip_address, prefix_mask, timestamp, as_number) VALUES(?,?,?,?,?)"
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

type BGPCaptureByTime struct {
	CassandraWriter
	timeBucketSeconds int64
}

type BGPCaptureByPrefixRange struct {
	CassandraWriter
	timeBucketSeconds int64
}

func (b BGPCaptureByTime) Write(request *pb.WriteRequest) error {
	//get message and convert timestamp to timeuuid
	errcount := 0
	msg := request.GetBgpCapture()
	if msg == nil {
		return fmt.Errorf("BGPCapture WriteRequest message was nil")
	}
	msgUp := msg.GetUpdate()
	if msgUp == nil {
		return fmt.Errorf("BGPUpdate in BGPCapture message was nil")
	}

	timestamp := gocql.UUIDFromTime(time.Unix(int64(msg.Timestamp), 0))
	advertisedPrefixes := []IPPrefix{}
	withdrawnPrefixes := []IPPrefix{}
	if msgUp.WithdrawnRoutes != nil {
		if len(msgUp.WithdrawnRoutes.Prefixes) != 0 {
			for _, wr := range msgUp.WithdrawnRoutes.Prefixes {
				ip, err := parseIp(*wr.Prefix)
				if err != nil {
					return err
				}
				withdrawnPrefixes = append(withdrawnPrefixes, IPPrefix{ip, uint8(wr.Mask)})
			}
		}
	}
	if msgUp.AdvertizedRoutes != nil {
		if len(msgUp.AdvertizedRoutes.Prefixes) != 0 {
			for _, ar := range msgUp.AdvertizedRoutes.Prefixes {
				ip, err := parseIp(*ar.Prefix)
				if err != nil {
					return err
				}
				advertisedPrefixes = append(advertisedPrefixes, IPPrefix{ip, uint8(ar.Mask)})
			}
		}
	}
	asp := getAsPath(*msgUp)
	colip, err := parseIp(*msg.LocalIp)
	if err != nil {
		return fmt.Errorf("Capture Local IP parsing error:%s", err)
	}
	peerip, err := parseIp(*msg.PeerIp)
	if err != nil {
		return fmt.Errorf("Capture Peer IP parsing error:%s", err)
	}
	var nhip net.IP
	if msgUp.Attrs != nil {
		if msgUp.Attrs.NextHop != nil {
			var err error
			nhip, err = parseIp(*msgUp.Attrs.NextHop)
			if err != nil {
				return fmt.Errorf("Capture NextHop IP parsing error:%s", err)
			}
		}
	}
	capbytes, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to serialize BGPCapture proto:%s", err)
	}

retry:
	if errcount < writeretries {
		err := b.cqlSession.Query(
			fmt.Sprintf(bgpCaptureByTimeStmt, b.keyspace),
			time.Unix(int64(msg.Timestamp)-(int64(msg.Timestamp)%b.timeBucketSeconds), 0),
			timestamp,
			advertisedPrefixes, //announced_prefixes
			asp,                //as_path
			colip,              //collector_ip_address
			nhip,               //NextHop next_hop
			peerip,             //peer_ip_address
			withdrawnPrefixes,  //withdrawn_prefixes
			capbytes,           //msg bytes
		).Exec()
		if err != nil {
			fmt.Printf("aprefix:%v\naspath:%v\ncip:%v\nnhop:%v\npip:%v\nwprefix:%v\n--------------------------\n",
				advertisedPrefixes,
				asp,
				colip,
				nhip,
				peerip,
				withdrawnPrefixes)
			errcount++
			goto retry
		}
	}

	return nil
}

func (b BGPCaptureByPrefixRange) Write(request *pb.WriteRequest) error {
	errcount := 0
	//get message and convert timestamp to timeuuid
	msg := request.GetBgpCapture()
	if msg == nil {
		return fmt.Errorf("BGPCapture WriteRequest message was nil")
	}
	msgUp := msg.GetUpdate()
	if msgUp == nil {
		return fmt.Errorf("BGPUpdate in BGPCapture message was nil")
	}
	asp := getAsPath(*msgUp)
	if len(asp) == 0 { // no as path info in that update
		return nil
	}
	timestamp := gocql.UUIDFromTime(time.Unix(int64(msg.Timestamp), 0))
	//fmt.Printf("by prefix msg with timestamp :%s\n", timestamp)

	if msgUp.AdvertizedRoutes != nil {
		if len(msgUp.AdvertizedRoutes.Prefixes) != 0 {
			for _, ar := range msgUp.AdvertizedRoutes.Prefixes {
				ip, err := parseIp(*ar.Prefix)
				if err != nil {
					return err
				}
			retry:
				if errcount < writeretries {
					err = b.cqlSession.Query(
						fmt.Sprintf(bgpCaptureByPrefixRangeStmt, b.keyspace),
						time.Unix(int64(msg.Timestamp)-(int64(msg.Timestamp)%b.timeBucketSeconds), 0),
						ip,
						ar.Mask,
						timestamp,
						asp[len(asp)-1],
					).Exec()

					if err != nil {
						errcount++
						goto retry
					}
				} else {
					return err
				}

			}
		}
	}
	return nil
}
