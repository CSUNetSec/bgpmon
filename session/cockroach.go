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
	//"sort"
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

type writeduration struct {
	dur time.Duration
	num int
}

type ByDuration []writeduration

func (b ByDuration) Len() int           { return len(b) }
func (b ByDuration) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b ByDuration) Less(i, j int) bool { return b[i].dur < b[j].dur }

func (w writeduration) String() string {
	return fmt.Sprintf("[Time:%v Opnum:%d]", w.dur, w.num)
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

// this will return the last AS in a AS-PATH.
// if the last path element is a set it will return the last AS in that set
func getLastAs(a pbbgp.BGPUpdate) (ret uint32) {
	if a.Attrs != nil {
		if len(a.Attrs.AsPath) > 0 {
			seg := a.Attrs.AsPath[len(a.Attrs.AsPath)-1]
			if seg.AsSeq != nil && len(seg.AsSeq) > 0 {
				ret = seg.AsSeq[len(seg.AsSeq)-1]
			} else if seg.AsSet != nil && len(seg.AsSet) > 0 {
				ret = seg.AsSet[len(seg.AsSet)-1]
			}
		}
	}
	return
}

type cockroachContext struct {
	db        *sql.DB
	stmupdate *sql.Stmt
	stmprefix *sql.Stmt
}

func NewCockroachSession(username string, hosts []string, workerCount uint32, certdir string, config CockroachConfig) (Sessioner, error) {

	var tablestr, dbstr string
	writers := make(map[pb.WriteRequest_Type][]Writer)
	for writerType, writerConfigs := range config.Writers {
		for _, writerConfig := range writerConfigs {
			fmt.Printf("registering writer %s\n", writerType)
			switch writerType {
			case "BGPCapture":
				//XXX this only works for one writer cause it sets the function scope vars that get passed on the goroutines
				tablestr, dbstr = writerConfig.Table, writerConfig.Database
				addWriter(writers, pb.WriteRequest_BGP_CAPTURE, BGPCapture{CockroachWriter{table: writerConfig.Table, database: writerConfig.Database}})
			default:
				return nil, errors.New(fmt.Sprintf("Unknown writer type %s for cockroach session", writerType))
			}
		}
	}

	workerChans := make([]chan *pb.WriteRequest, len(hosts))

	for i := 0; i < len(hosts); i++ {
		workerChan := make(chan *pb.WriteRequest)
		go func(wc chan *pb.WriteRequest, id int) {
			//num := 0
			//writeTimes := new(ByDuration)
			var (
				stupdate, stprefix *sql.Stmt
				err                error
			)
			host := hosts[id%len(hosts)]
			db, err := sql.Open("postgres", fmt.Sprintf("postgresql://%s@%s:26257/?sslmode=verify-full&sslcert=%s/node.cert&sslrootcert=%s/ca.cert&sslkey=%s/node.key",
				username, host, certdir, certdir, certdir))
			if err != nil {
				log.Errl.Printf("Unable to open connection to %s error:%s", host, err)
				return
			}
			db.SetMaxIdleConns(10)
			if err := db.Ping(); err != nil {
				log.Errl.Printf("Unable to start connection to %s error:%s", host, err)
				return
			}
			if stupdate, err = db.Prepare(fmt.Sprintf(bgpCaptureStmt, dbstr, tablestr)); err != nil {
				log.Errl.Printf("Unable to prepare update statmements on host:%s error:%s", host, err)
				return
			}
			if stprefix, err = db.Prepare(fmt.Sprintf(bgpPrefixStmt, dbstr, "prefixes")); err != nil {
				log.Errl.Printf("Unable to prepare prefix statmements on host:%s error:%s", host, err)
				return
			}
			cc := &cockroachContext{db, stupdate, stprefix}
			workchan := make(chan *pb.WriteRequest)
			for j := 0; j < int(workerCount); j++ {
				go Write(cc, workchan)
			}
			for writeRequest := range wc {
				//ts := time.Now()
				workchan <- writeRequest
				//*writeTimes = append(*writeTimes, writeduration{time.Since(ts), num})
				//num++

			}
			//log.Debl.Printf("sorting write times")
			//sort.Sort(writeTimes)
			//log.Debl.Printf("%v", writeTimes)
			close(workchan)
			stupdate.Close()
			stprefix.Close()
			db.Close()
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
	//prefixes (prefix_id SERIAL PRIMARY KEY, update_id INT, ip_address BYTES, ip_address_str STRING, mask INT, source_as INT, is_withdrawn BOOL);
	bgpCaptureStmt = "INSERT INTO %s.%s(update_id, timestamp, collector_ip, collector_ip_str, peer_ip, peer_ip_str, as_path, next_hop, next_hop_str, protomsg) VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING update_id;"
	bgpPrefixStmt  = "INSERT INTO %s.%s(prefix_id, update_id, ip_address, ip_address_str, mask, source_as, timestamp, is_withdrawn) VALUES (DEFAULT, $1, $2, $3, $4, $5, $6, $7);"
)

type CockroachWriter struct {
	cc       *cockroachContext
	table    string
	database string
}

type BGPCapture struct {
	CockroachWriter
}

func (b BGPCapture) Write(request *pb.WriteRequest) error {
	return nil
}

func Write(cc *cockroachContext, wchan <-chan *pb.WriteRequest) {
	for request := range wchan {
		msg := request.GetBgpCapture()
		if msg == nil {
			log.Errl.Printf("BGPCapture WriteRequest message was nil")
			continue
		}
		msgUp := msg.GetUpdate()
		if msgUp == nil {
			log.Errl.Printf("BGPUpdate in BGPCapture message was nil")
			continue
		}

		timestamp := time.Unix(int64(msg.Timestamp), 0)
		colip, colipstr, err := parseIpToIPString(*msg.LocalIp)
		if err != nil {
			log.Errl.Printf("Capture Local IP parsing error:%s", err)
			continue
		}
		peerip, peeripstr, err := parseIpToIPString(*msg.PeerIp)
		if err != nil {
			log.Errl.Printf("Capture Peer IP parsing error:%s", err)
			continue
		}
		capbytes, err := proto.Marshal(msg)
		if err != nil {
			log.Errl.Printf("failed to serialize BGPCapture proto:%s", err)
			continue
		}
		aspstr := getAsPathString(*msgUp)
		lastas := getLastAs(*msgUp)
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
					continue
				}
			}
		}
		var id int64
		row := cc.stmupdate.QueryRow(
			timestamp, []byte(colip), colipstr, []byte(peerip), peeripstr, aspstr, []byte(nhip), nhipstr, capbytes)
		if errid := row.Scan(&id); errid != nil {
			log.Errl.Printf("error in fetching id from last insert:%s", errid)
			continue
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
				_, errpref := cc.stmprefix.Exec(
					id, []byte(ip), ipstr, mask, lastas, timestamp, true)
				if errpref != nil {
					log.Errl.Printf("error:%s in inserting in prefix table", errpref)
					continue
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
				_, errpref := cc.stmprefix.Exec(
					id, []byte(ip), ipstr, mask, lastas, timestamp, false)
				if errpref != nil {
					log.Errl.Printf("error:%s in inserting in prefix table", errpref)
					continue
				}
			}
		}
	}
	log.Debl.Printf("writer exiting")

	return
}

func (b BGPCapture) WriteCon(cc *cockroachContext, request *pb.WriteRequest) error {
	b.cc = cc
	return b.Write(request)
}
