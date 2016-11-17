package session

import (
	"database/sql"
	"errors"
	"fmt"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon"
	pbcom "github.com/CSUNetSec/netsec-protobufs/common"
	pbbgp "github.com/CSUNetSec/netsec-protobufs/protocol/bgp"
	"github.com/golang/protobuf/proto"
	_ "github.com/lib/pq"
	"net"
	"time"
)

type CockroachConfig struct {
	Writers map[string][]CockroachWriterConfig
	CertDir string
}

type CockroachWriterConfig struct {
	Table    string
	Database string
}

type CockroachSession struct {
	*Session
	SqlSession *sql.DB
}

func parseIpString(a pbcom.IPAddressWrapper) (string, error) {
	if len(a.Ipv6) != 0 {
		return net.IP(a.Ipv6).String(), nil
	} else if len(a.Ipv4) != 0 {
		return net.IP(a.Ipv4).To4().String(), nil
	} else {
		return "", fmt.Errorf("IP Prefix that is neither v4 or v6 found in BGPCapture msg")
	}
}

func getAsPathString(a pbbgp.BGPUpdate) string {
	ret := make([]uint32, 0)
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
	retstr := fmt.Sprintf("%s", ret)
	return retstr
}

func NewCockroachSession(username string, hosts []string, workerCount uint32, certdir string, config CockroachConfig) (Sessioner, error) {
	db, err := sql.Open("postgres", fmt.Sprintf("postgresql://%s@%s:26257/?sslmode=verify-full&sslcert=%s/node.cert&sslrootcert=%s/ca.cert&sslkey=%s/node.key",
		username, hosts[0], certdir, certdir, certdir))
	if err != nil {
		return nil, err
	}

	writers := make(map[pb.WriteRequest_Type][]Writer)
	for writerType, writerConfigs := range config.Writers {
		for _, writerConfig := range writerConfigs {
			fmt.Printf("registering writer %s\n", writerType)
			switch writerType {
			case "BGPCapture":
				addWriter(writers, pb.WriteRequest_BGP_CAPTURE, BGPCapture{CockroachWriter{db, writerConfig.Table, writerConfig.Database}})
			default:
				return nil, errors.New(fmt.Sprintf("Unknown writer type %s for cockroach session", writerType))
			}
		}
	}

	session, err := NewSession(writers, workerCount)
	if err != nil {
		return nil, err
	}

	return CockroachSession{&session, db}, nil
}

func (c CockroachSession) Close() error {
	c.SqlSession.Close()
	return nil
}

/*
 * Writers
 */

const (
	bgpCaptureStmt = "INSERT INTO %s.%s(timestamp, collector_ip, peer_ip, as_path, next_hop, advertised_prefixes, withdrawn_prefixes, protomsg) VALUES($1, $2, $3, $4, $5, $6, $7, $8);"
)

type CockroachWriter struct {
	sqlSession *sql.DB
	table      string
	database   string
}

type BGPCapture struct {
	CockroachWriter
}

func (b BGPCapture) Write(request *pb.WriteRequest) error {
	msg := request.GetBgpCapture()
	if msg == nil {
		return fmt.Errorf("BGPCapture WriteRequest message was nil")
	}
	msgUp := msg.GetUpdate()
	if msgUp == nil {
		return fmt.Errorf("BGPUpdate in BGPCapture message was nil")
	}

	timestamp := time.Unix(int64(msg.Timestamp), 0)
	colip, err := parseIpString(*msg.LocalIp)
	if err != nil {
		return fmt.Errorf("Capture Local IP parsing error:%s", err)
	}
	peerip, err := parseIpString(*msg.PeerIp)
	if err != nil {
		return fmt.Errorf("Capture Peer IP parsing error:%s", err)
	}
	capbytes, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to serialize BGPCapture proto:%s", err)
	}
	if _, errdb := b.sqlSession.Exec(fmt.Sprintf(bgpCaptureStmt, b.database, b.table),
		timestamp, colip, peerip, nil, nil, nil, nil, capbytes); errdb != nil {
		return errdb
	}

	return nil
}
