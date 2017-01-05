package session

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/CSUNetSec/bgpmon/log"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon"
	pbcom "github.com/CSUNetSec/netsec-protobufs/common"
	pbbgp "github.com/CSUNetSec/netsec-protobufs/protocol/bgp"
	"github.com/golang/protobuf/proto"
	_ "github.com/lib/pq"
	"github.com/rogpeppe/fastuuid"
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
	username string
	hosts    []string
	port     uint32
	certdir  string
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
	uuidgen   *fastuuid.Generator
	stmtupstr string
	stmtprstr string
}

func NewCockroachSession(username string, hosts []string, port uint32, workerCount uint32, certdir string, config CockroachConfig) (Sessioner, error) {
	var tablestr, dbstr string
	//var tablestr string
	uug := fastuuid.MustNewGenerator()
	writers := make(map[pb.WriteRequest_Type][]Writer)
	for writerType, writerConfigs := range config.Writers {
		for _, writerConfig := range writerConfigs {
			fmt.Printf("registering writer %s\n", writerType)
			switch writerType {
			case "BGPCapture":
				//XXX pass also the prefix table str from the client cause now we hardcode it
				tablestr, dbstr = writerConfig.Table, writerConfig.Database
				addWriter(writers, pb.WriteRequest_BGP_CAPTURE, BGPCapture{CockroachWriter{table: writerConfig.Table, database: writerConfig.Database}})
			default:
				return nil, errors.New(fmt.Sprintf("Unknown writer type %s for cockroach session", writerType))
			}
		}
	}

	workerChans := make([]chan *pb.WriteRequest, 1) // all the workers now will listen on this one chanel
	workerChan := make(chan *pb.WriteRequest)       //this
	workerChans[0] = workerChan

	cccontexts := []*cockroachContext{}
	for i := 0; i < len(hosts); i++ {
		host := hosts[i]
		db, err := sql.Open("postgres", fmt.Sprintf("postgresql://%s@%s:%d/?sslmode=verify-full&sslcert=%s/root.cert&sslrootcert=%s/ca.cert&sslkey=%s/root.key",
			username, host, port, certdir, certdir, certdir))
		//db, err := sql.Open("postgres", fmt.Sprintf("postgresql://%s:papakia@%s:%d/bgpmon?sslmode=disable", username, host, port))
		if err != nil {
			log.Errl.Printf("Unable to open connection to %s error:%s", host, err)
			continue
		}
		//db.SetMaxIdleConns(10)
		stmtupstr := fmt.Sprintf(bgpCaptureStmt1, dbstr, tablestr)
		//stmtupstr := fmt.Sprintf(bgpCaptureStmt1, tablestr) //postgres
		stmtprstr := fmt.Sprintf(bgpPrefixStmt1, dbstr, "prefixes") //XXX hardcoded prefixes
		//stmtprstr := fmt.Sprintf(bgpPrefixStmt1, "prefixes") //posgres
		cccontexts = append(cccontexts, &cockroachContext{db, nil, nil, uug, stmtupstr, stmtprstr})
	}
	for j := 0; j < int(workerCount); j++ {
		go Write(cccontexts[j%len(cccontexts)], workerChans[0])
	}

	session := Session{workerChans, 0}

	return CockroachSession{&session, username, hosts, port, certdir}, nil
}

func (c CockroachSession) Close() error {
	for _, ch := range c.workerChans {
		close(ch)
	}
	return nil
}

func (c CockroachSession) GetDbConnection() (*sql.DB, error) {
	return sql.Open("postgres", fmt.Sprintf("postgresql://%s@%s:%d/?sslmode=verify-full&sslcert=%s/root.cert&sslrootcert=%s/ca.cert&sslkey=%s/root.key",
		c.username, c.hosts[0], c.port, c.certdir, c.certdir, c.certdir))
}

/*
 * Writers
 */

const (
	//relevant tables in schema
	//updates (update_id SERIAL PRIMARY KEY, timestamp TIMESTAMP, collector_ip BYTES, collector_ip_str STRING, peer_ip BYTES, peer_ip_str STRING, as_path STRING, next_hop BYTES, next_hop_str STRING, protomsg BYTES);
	//prefixes (prefix_id SERIAL PRIMARY KEY, update_id INT, ip_address BYTES, ip_address_str STRING, mask INT, source_as INT, is_withdrawn BOOL);
	//bgpCaptureStmt  = "INSERT INTO %s.%s(update_id, timestamp, collector_ip, collector_ip_str, peer_ip, peer_ip_str, as_path, next_hop, next_hop_str, protomsg) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
	bgpCaptureStmt1 = "INSERT INTO %s.%s(update_id, timestamp, collector_ip, peer_ip, as_path, next_hop, protomsg) VALUES"
	//bgpCaptureStmt1 = "INSERT INTO %s(update_id, timestamp, collector_ip, collector_ip_str, peer_ip, peer_ip_str, as_path, next_hop, next_hop_str, protomsg) VALUES"
	//bgpPrefixStmt  = "INSERT INTO %s.%s(prefix_id, update_id, ip_address, ip_address_str, mask, source_as, timestamp, is_withdrawn) VALUES (DEFAULT, $1, $2, $3, $4, $5, $6, $7);"
	bgpPrefixStmt1 = "INSERT INTO %s.%s(prefix_id, update_id, ip_address, mask, source_as, timestamp, is_withdrawn) VALUES"
	//bgpPrefixStmt1  = "INSERT INTO %s(prefix_id, update_id, ip_address, ip_address_str, mask, source_as, timestamp, is_withdrawn) VALUES"
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

type buffer struct {
	stmt     string
	buf      []interface{}
	size     int
	initstmt string
	cc       *cockroachContext
	firstdef bool
}

func newbuffer(stmt string, sz int, cc *cockroachContext, firstdef bool) *buffer {
	return &buffer{stmt, make([]interface{}, 0, sz), sz, stmt, cc, firstdef}
}

func valstrgen(start, amount int, last bool, firstdef bool) string {
	ret := ""
	if firstdef {
		ret += "(DEFAULT,"
	} else {
		ret += "("
	}
	for i := 1; i <= amount; i++ {
		if i == amount {
			ret += fmt.Sprintf("$%d", start+i)
		} else {
			ret += fmt.Sprintf("$%d,", start+i)
		}
	}
	if last {
		ret += ");"
	} else {
		ret += "),"
	}
	return ret
}

func (b *buffer) add(vals ...interface{}) {
	//fmt.Printf("adding %d\n", len(vals))
	if len(vals) > b.size {
		log.Errl.Printf("can' add more elements at one step to the buffer than it's size")
		return
	}
	if b.size-len(b.buf)-len(vals) < 0 {
		//fmt.Printf("calling flush from add\n")
		b.flush(false)
		b.add(vals...)
	} else {
		if b.size-len(b.buf)-(2*len(vals)) < 0 { // take care of predicting the last statement
			b.stmt += valstrgen(len(b.buf), len(vals), true, b.firstdef)
		} else {
			b.stmt += valstrgen(len(b.buf), len(vals), false, b.firstdef)
		}
		for i := range vals {
			b.buf = append(b.buf, vals[i])
		}
	}
}

func (b *buffer) flush(notfull bool) {
	if len(b.buf) > 0 {
		if notfull {
			b.stmt = b.stmt[:len(b.stmt)-1] + ";"
		}
		//log.Debl.Printf("trying to run query on flush")
		_, err := b.cc.db.Exec(b.stmt, b.buf...)
		//log.Debl.Printf("done")
		if err != nil {
			log.Errl.Printf("executed query:%s with vals:%+v error:%s", b.stmt, b.buf, err)
		}
		b.buf = nil
		b.stmt = b.initstmt
	}
	//log.Debl.Printf("buffer flush:%+v\n", b)
}

//write buffers messages that arrive on wchan and writes them if they hit a number.
//at the same time it has a ticker that checks if it's getting messages from the cli.
//if the ticker detects that it didn't get any messages it fluses the queue.
//when wchan closes it will detect it and close the ticket too so the goroutine can die
func Write(cc *cockroachContext, wchan <-chan *pb.WriteRequest) {
	ticker := time.NewTicker(10 * time.Second)
	idleticks := 0
	upbuf := newbuffer(cc.stmtupstr, 1000, cc, false)  // fits around 100 objects do not include a default first arg in stmt
	prefbuf := newbuffer(cc.stmtprstr, 3000, cc, true) // fits around 300 objects include a default first arg in stmt
	for {
		select {
		case request, wchopen := <-wchan:
			//check if someone closed our chan and signal the ticker
			if !wchopen {
				//XXX flush
				fmt.Printf("chan closed. calling flush\n")
				upbuf.flush(true)
				prefbuf.flush(true)
				wchan = nil
				ticker.Stop()
				cc.db.Close()
				break
			}
			idleticks = 0 //reset the idle counter
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
			colip, _, err := parseIpToIPString(*msg.LocalIp)
			if err != nil {
				log.Errl.Printf("Capture Local IP parsing error:%s", err)
				continue
			}
			peerip, _, err := parseIpToIPString(*msg.PeerIp)
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
			)
			if msgUp.Attrs != nil {
				if msgUp.Attrs.NextHop != nil {
					var errnh error
					nhip, _, errnh = parseIpToIPString(*msgUp.Attrs.NextHop)
					if errnh != nil {
						log.Errl.Printf("Capture NextHop IP parsing error:%s", errnh)
						continue
					}
				}
			}
			var id string
			uuid := cc.uuidgen.Next()
			id = hex.EncodeToString(uuid[:])
			upbuf.add(id, timestamp, []byte(colip), []byte(peerip), aspstr, []byte(nhip), capbytes)
			/*row := cc.stmupdate.QueryRow(
				timestamp, []byte(colip), colipstr, []byte(peerip), peeripstr, aspstr, []byte(nhip), nhipstr, capbytes)
			if errid := row.Scan(&id); errid != nil {
				log.Errl.Printf("error in fetching id from last insert:%s", errid)
				continue
			}*/
			if msgUp.WithdrawnRoutes != nil && len(msgUp.WithdrawnRoutes.Prefixes) != 0 {
				for _, wr := range msgUp.WithdrawnRoutes.Prefixes {
					ip, _, err := parseIpToIPString(*wr.Prefix)
					if err != nil {
						log.Errl.Printf("error:%s parsing withdrawn prefix", err)
						continue
					}
					mask := int(wr.Mask)
					///XXX hardcoded table
					prefbuf.add(id, []byte(ip), mask, lastas, timestamp, true)
					/*_, errpref := cc.stmprefix.Exec(
						id, []byte(ip), ipstr, mask, lastas, timestamp, true)
					if errpref != nil {
						log.Errl.Printf("error:%s in inserting in prefix table", errpref)
						continue
					}*/
				}
			}

			if msgUp.AdvertizedRoutes != nil && len(msgUp.AdvertizedRoutes.Prefixes) != 0 {
				for _, ar := range msgUp.AdvertizedRoutes.Prefixes {
					ip, _, err := parseIpToIPString(*ar.Prefix)
					if err != nil {
						log.Errl.Printf("error:%s parsing advertized prefix", err)
						continue
					}
					mask := int(ar.Mask)
					///XXX hardcoded table
					prefbuf.add(id, []byte(ip), mask, lastas, timestamp, false)
					/*_, errpref := cc.stmprefix.Exec(
						id, []byte(ip), ipstr, mask, lastas, timestamp, false)
					if errpref != nil {
						log.Errl.Printf("error:%s in inserting in prefix table", errpref)
						continue
					}*/
				}
			}
		case <-ticker.C:
			idleticks++
			if idleticks > 1 { // 10 or more seconds passed since a msg arrived. flush
				//log.Debl.Printf("flushing due to inacivity\n")
				upbuf.flush(true)
				prefbuf.flush(true)
				idleticks = 0
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
