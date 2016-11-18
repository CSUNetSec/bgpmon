package session

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/CSUNetSec/bgpmon/log"
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

	writers := make(map[pb.WriteRequest_Type][]Writer)
	for writerType, writerConfigs := range config.Writers {
		for _, writerConfig := range writerConfigs {
			fmt.Printf("registering writer %s\n", writerType)
			switch writerType {
			case "BGPCapture":
				addWriter(writers, pb.WriteRequest_BGP_CAPTURE, BGPCapture{CockroachWriter{table: writerConfig.Table, database: writerConfig.Database}})
			default:
				return nil, errors.New(fmt.Sprintf("Unknown writer type %s for cockroach session", writerType))
			}
		}
	}

	workerChans := make([]chan *pb.WriteRequest, workerCount)

	for i := 0; i < int(workerCount); i++ {
		workerChan := make(chan *pb.WriteRequest)
		go func(wc chan *pb.WriteRequest, id int) {
			host := hosts[id%len(hosts)]
			db, err := sql.Open("postgres", fmt.Sprintf("postgresql://%s@%s:26257/?sslmode=verify-full&sslcert=%s/node.cert&sslrootcert=%s/ca.cert&sslkey=%s/node.key",
				username, host, certdir, certdir, certdir))
			if err != nil {
				log.Errl.Printf("Unable to open connection to %s error:%s", host, err)
				return
			}
			for {
				select {
				case writeRequest, open := <-wc:
					writers, exists := writers[writeRequest.Type]
					if !exists {
						//TODO get an error message back somehow
						//panic(errors.New(fmt.Sprintf("Unable to write type '%v' because it doesn't exist", writeRequest.Type)))
						log.Errl.Printf("Unable to write type '%v' because it doesn't exist", writeRequest.Type)
					}

					for _, writer := range writers {
						//fmt.Printf("writing in writer :%v\n", writer)
						//XXX: hack. force it to be a bgpcapture.
						bc := writer.(BGPCapture)
						if err := bc.WriteCon(db, writeRequest); err != nil {
							log.Errl.Printf("error from worker for write request:%+v on writer:%+v error:%s\n", writeRequest, writer, err)
							break
						}
					}
					if !open {
						wc = nil
					}
					if wc == nil {
						break
					}
				}
			}
			log.Debl.Printf("worker exiting")
		}(workerChan, i)

		workerChans[i] = workerChan
	}

	session := Session{workerChans, 0}

	return CockroachSession{&session}, nil
}

func (c CockroachSession) Close() error {
	for _, ch := range c.workerChans {
		close(ch)
	}
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

func (b BGPCapture) WriteCon(con *sql.DB, request *pb.WriteRequest) error {
	b.sqlSession = con
	return b.Write(request)
}
