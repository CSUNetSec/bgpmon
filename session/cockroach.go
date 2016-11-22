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

func parseIpToIPString(a pbcom.IPAddressWrapper) (net.IP, string, error) {
	if len(a.Ipv6) != 0 {
		return net.IP(a.Ipv6), net.IP(a.Ipv6).String(), nil
	} else if len(a.Ipv4) != 0 {
		return net.IP(a.Ipv4), net.IP(a.Ipv4).String(), nil
	} else {
		return nil, "", fmt.Errorf("IP Prefix that is neither v4 or v6 found in BGPCapture msg")
	}
}

func getAsPathString(a pbbgp.BGPUpdate) string {
	ret := ""
	if a.Attrs != nil {
		for _, seg := range a.Attrs.AsPath {
			if seg.AsSeq != nil {
				for _, as := range seg.AsSeq {
					ret = ret + fmt.Sprintf(" %d ", as)
				}
			}
			if seg.AsSet != nil {
				for _, as := range seg.AsSet {
					ret = ret + fmt.Sprintf(" %d ", as)
				}
			}
		}
	}
	return ret
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
			for writeRequest := range wc {
				log.Debl.Printf("go a write op")
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
					log.Debl.Printf("calling write")
					if err := bc.WriteCon(db, writeRequest); err != nil {
						log.Errl.Printf("error from worker for write request:%+v on writer:%+v error:%s\n", writeRequest, writer, err)
						break
					}
					log.Debl.Printf("ended write")
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
	//relevant tables in schema
	//updates (update_id SERIAL PRIMARY KEY, timestamp TIMESTAMP, collector_ip BYTES, collector_ip_str STRING, peer_ip BYTES, peer_ip_str STRING, as_path STRING, next_hop BYTES, next_hop_str STRING, protomsg BYTES);
	//prefixes (prefix_id SERIAL PRIMARY KEY, update_id INT, ip_address BYTES, ip_address_str STRING, mask INT, is_withdrawn BOOL);
	bgpCaptureStmt = "INSERT INTO %s.%s(update_id, timestamp, collector_ip, collector_ip_str, peer_ip, peer_ip_str, as_path, next_hop, next_hop_str, protomsg) VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING update_id;"
	bgpPrefixStmt  = "INSERT INTO %s.%s(prefix_id, update_id, ip_address, ip_address_str, mask, is_withdrawn) VALUES (DEFAULT, $1, $2, $3, $4, $5);"
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
	colip, colipstr, err := parseIpToIPString(*msg.LocalIp)
	if err != nil {
		return fmt.Errorf("Capture Local IP parsing error:%s", err)
	}
	peerip, peeripstr, err := parseIpToIPString(*msg.PeerIp)
	if err != nil {
		return fmt.Errorf("Capture Peer IP parsing error:%s", err)
	}
	capbytes, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to serialize BGPCapture proto:%s", err)
	}
	aspstr := getAsPathString(*msgUp)
	//XXX func this
	var (
		nhip    net.IP
		nhipstr string
	)
	if msgUp.Attrs != nil {
		if msgUp.Attrs.NextHop != nil {
			var errnh error
			nhip, nhipstr, errnh = parseIpToIPString(*msgUp.Attrs.NextHop)
			if errnh != nil {
				log.Errl.Printf("Capture NextHop IP parsing error:%s", errnh)
			}
		}
	}
	var id, idcnt int
	if rows, errdb := b.sqlSession.Query(fmt.Sprintf(bgpCaptureStmt, b.database, b.table),
		timestamp, []byte(colip), colipstr, []byte(peerip), peeripstr, aspstr, []byte(nhip), nhipstr, capbytes); errdb != nil {
		return fmt.Errorf("error inserting in update table:%s", errdb)
	} else {
		defer rows.Close()
		for rows.Next() {
			if idcnt++; idcnt > 1 {
				log.Debl.Printf("Query returned more than one unique ids")
				break
			}
			if errid := rows.Scan(&id); errid != nil {
				return fmt.Errorf("error in fetching id from last insert:%s", errid)
			}
		}
	}
	if msgUp.WithdrawnRoutes != nil && len(msgUp.WithdrawnRoutes.Prefixes) != 0 {
		for _, wr := range msgUp.WithdrawnRoutes.Prefixes {
			ip, ipstr, err := parseIpToIPString(*wr.Prefix)
			if err != nil {
				log.Errl.Printf("error:%s parsing withdrawn prefix", err)
				continue
			}
			mask := int(wr.Mask)
			///XXX hardcoded table
			_, errpref := b.sqlSession.Exec(fmt.Sprintf(bgpPrefixStmt, b.database, "prefixes"),
				id, []byte(ip), ipstr, mask, true)
			if errpref != nil {
				log.Errl.Printf("error:%s in inserting in prefix table", errpref)
			}
		}
	}

	if msgUp.AdvertizedRoutes != nil && len(msgUp.AdvertizedRoutes.Prefixes) != 0 {
		for _, ar := range msgUp.AdvertizedRoutes.Prefixes {
			ip, ipstr, err := parseIpToIPString(*ar.Prefix)
			if err != nil {
				log.Errl.Printf("error:%s parsing advertized prefix", err)
				continue
			}
			mask := int(ar.Mask)
			///XXX hardcoded table
			_, errpref := b.sqlSession.Exec(fmt.Sprintf(bgpPrefixStmt, b.database, "prefixes"),
				id, []byte(ip), ipstr, mask, false)
			if errpref != nil {
				log.Errl.Printf("error:%s in inserting in prefix table", errpref)
			}
		}
	}
	log.Debl.Printf("inserted msg")

	return nil
}

func (b BGPCapture) WriteCon(con *sql.DB, request *pb.WriteRequest) error {
	b.sqlSession = con
	return b.Write(request)
}
