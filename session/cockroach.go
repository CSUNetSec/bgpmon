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
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

//db operation string templates.
const (
	insertCaptureTMPL        = "INSERT INTO %s.%s (update_id, timestamp, collector_ip, peer_ip, as_path, next_hop, origin_as, protomsg) VALUES"
	selectDBbyColAndDateTMPL = "SELECT dbname, dateFrom, dateTo FROM %s.dbs where collector = $1 and datefrom <= $2 and dateto > $2"
	createCaptureTableTMPL   = "CREATE TABLE IF NOT EXISTS %s.%s (update_id STRING PRIMARY KEY, timestamp TIMESTAMP, collector_ip BYTES, peer_ip BYTES, as_path STRING, next_hop BYTES, origin_as INT, protomsg BYTES);"
	insertDBbyColAndDateTMPL = "UPSERT INTO %s.dbs(dbname, collector, datefrom, dateto) VALUES ($1, $2, $3, $4)"
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
	dbs      []*sql.DB
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

//cockroach context holds some info for the writers
type cockroachContext struct {
	db           *sql.DB             //the underlying db connection that is shared accros all write goroutines
	uuidgen      *fastuuid.Generator //a uuid generator reference that is shared accross all write goroutines
	dbstr        string              //the dbname string provided by the configuration file of bgpmond
	selectDbStmt string              //this statement selects the table to write captures to depending on collector and date
	insertDbStmt string              //this statement inserts the table name and date ranges in to bookkeeping table named [databasename].dbs
	contid       int
}

func newCockcroachContext(db *sql.DB, uuidgen *fastuuid.Generator, dbstr string, contid int) *cockroachContext {
	ret := &cockroachContext{}
	ret.db, ret.uuidgen, ret.dbstr = db, uuidgen, dbstr
	ret.selectDbStmt = fmt.Sprintf(selectDBbyColAndDateTMPL, dbstr)
	ret.insertDbStmt = fmt.Sprintf(insertDBbyColAndDateTMPL, dbstr)
	ret.contid = contid
	return ret
}

func NewCockroachSession(username string, hosts []string, port uint32, workerCount uint32, certdir string, config CockroachConfig) (Sessioner, error) {
	var dbstr string
	log.Debl.Printf("hosts:%v\n", hosts)
	uug := fastuuid.MustNewGenerator()
	writers := make(map[pb.WriteRequest_Type][]Writer)
	for writerType, writerConfigs := range config.Writers {
		for _, writerConfig := range writerConfigs {
			fmt.Printf("registering writer %s\n", writerType)
			switch writerType {
			case "BGPCapture":
				dbstr = writerConfig.Database
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
	dbs := []*sql.DB{}
	ccids := 0
	for i := 0; i < len(hosts); i++ {
		host := hosts[i]
		db, err := OpenDbConnection(host, port, certdir, username)
		if err != nil {
			log.Errl.Printf("Unable to open connection to hosts %s error:%s", hosts, err)
			continue
		}
		dbs = append(dbs, db)
		//db.SetMaxIdleConns(10)
		ccids++
		cccontexts = append(cccontexts, newCockcroachContext(db, uug, dbstr, ccids))
	}
	for j := 0; j < int(workerCount); j++ {
		go Write(cccontexts[j%len(cccontexts)], workerChans[0])
	}

	session := Session{workerChans, 0}

	return CockroachSession{&session, username, hosts, port, certdir, dbs}, nil
}

func (c CockroachSession) Close() error {
	for _, ch := range c.workerChans {
		close(ch)
	}
	return nil
}

func genOpenHostStr(hosts []string, port uint32) string {
	ret := ""
	for i, v := range hosts {
		if i == len(hosts)-1 {
			ret += fmt.Sprintf("%s:%d", v, port)
		} else {
			ret += fmt.Sprintf("%s:%d,", v, port)
		}
	}
	return ret
}

func OpenDbConnection(host string, port uint32, certdir string, username string) (*sql.DB, error) {
	return sql.Open("postgres", fmt.Sprintf("postgresql://%s:%d/?user=%s&sslmode=verify-full&sslcert=%s/client.%s.crt&sslrootcert=%s/ca.crt&sslkey=%s/client.%s.key&statement_timeout=10000",
		host, port, username, certdir, username, certdir, certdir, username))
}

func (c CockroachSession) GetRandDbConnection() *sql.DB {
	return c.dbs[rand.Intn(len(c.dbs))]
}

/*
 * Writers
 */

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

type openBuf struct {
	buf      *buffer
	dateFrom time.Time
	dateTo   time.Time
	col      string
}

func NewOpenBuf(buf *buffer, df, dt time.Time, col string) openBuf {
	return openBuf{
		buf:      buf,
		dateFrom: df,
		dateTo:   dt,
		col:      col,
	}
}

type openBuffers struct {
	bufs []openBuf
	cc   *cockroachContext
}

func NewOpenBuffers(cc *cockroachContext) *openBuffers {
	return &openBuffers{
		bufs: make([]openBuf, 0),
		cc:   cc,
	}
}

//gives back the start and end of that day in UTC.
func getDayBounds(t time.Time) (r1 time.Time, r2 time.Time) {
	r1 = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	r2 = r1.AddDate(0, 0, 1).Add(time.Nanosecond * -1)
	return
}

func (o *openBuffers) Find(col string, tstamp time.Time) (openBuf, bool, error) {
	var (
		tablename        string
		dateFrom, dateTo time.Time
	)
	//IMPORTANT all locations for time are set to UTC and all periods ( . ) and columns ( : ) in IPs become underscores ( _ )
	//replace the dots and columns in the collector string
	col = strings.Replace(col, ".", "_", -1)
	col = strings.Replace(col, ":", "_", -1)
	//set the location to utc
	tstamp = tstamp.UTC()
	//try to see if we have the buffer for this collector/timerange open already
	for _, b := range o.bufs {
		if b.col == col && (tstamp.After(b.dateFrom) || tstamp.Equal(b.dateFrom)) && tstamp.Before(b.dateTo) {
			//log.Debl.Printf("matched created buffer")
			return b, true, nil
		}
	}
	log.Debl.Printf("CID:%d Querying Find Openbuf for collector:%s time:%s", o.cc.contid, col, tstamp)
	tx, err := o.cc.db.Begin()
	if err != nil {
		log.Errl.Printf("err starting transaction :%s", err)
		return openBuf{}, false, err
	}
	defer tx.Commit()
	row := tx.QueryRow(o.cc.selectDbStmt, col, tstamp)
	err = row.Scan(&tablename, &dateFrom, &dateTo)
	if err == sql.ErrNoRows { //we need to create an entry for this collector/month
		d1, d2 := getDayBounds(tstamp)
		//the time in the tablename is formated as YYYYMMDD since we have one table per
		//collector per day
		newtablename := fmt.Sprintf("captures_%s_%s", col, tstamp.Format("20060102"))
		//First create the receiving table otherwise the transaction will fail.
		//this uses the template createCaptureTableTMPL and populates the tablename and new table name
		rid, errq := tx.Exec(fmt.Sprintf(createCaptureTableTMPL, o.cc.dbstr, newtablename))
		if errq != nil {
			log.Errl.Printf("CID:%d error creating new table for captures:%s", o.cc.contid, errq)
			return openBuf{}, false, errq
		} else {
			log.Debl.Printf("CID:%d created new table %s for captures", o.cc.contid, newtablename)
		}
		//no insert the name of the table to the db record table
		rid, errq = tx.Exec(o.cc.insertDbStmt, newtablename, col, d1, d2)
		if errq != nil {
			log.Errl.Printf("error adding db row :%s", errq)
			return openBuf{}, false, errq
		}
		raffect, _ := rid.RowsAffected()
		log.Debl.Printf("CID:%d added row with for col:%s from:%v to:%v id:%d to known dbs", o.cc.contid, col, d1, d2, raffect)
		tablename = newtablename
	} else if err != nil { //error.
		log.Errl.Printf("CID:%d querying the dbs table error:%s", o.cc.contid, err)
		return openBuf{}, false, err
	} else {
		log.Debl.Printf("CID:%d found an existing database with name %s\n", o.cc.contid, tablename)
	}
	upstmt := fmt.Sprintf(insertCaptureTMPL, o.cc.dbstr, tablename)
	upbuf := newbuffer(upstmt, 3000, o.cc, false) // fits around 100 objects per write
	obuf := NewOpenBuf(upbuf, dateFrom, dateTo, col)
	//add it to the known bufs
	o.bufs = append(o.bufs, obuf)
	return obuf, true, nil
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
	if b.buf != nil && len(b.buf) > 0 {
		if notfull {
			b.stmt = b.stmt[:len(b.stmt)-1] + ";"
		}
		//log.Debl.Printf("trying to run query on flush")
		t1 := time.Now()
		tx, _ := b.cc.db.Begin()
		_, err := tx.Exec(b.stmt, b.buf...)
		//log.Debl.Printf("done")
		if err != nil {
			log.Errl.Printf("executed query:%s with vals:%+v error:%s", b.stmt, b.buf, err)
			tx.Rollback()
		}
		tx.Commit()
		log.Debl.Printf("DB TX TIME:%s", time.Since(t1))
		b.buf = nil
		b.stmt = b.initstmt
	}
	//log.Debl.Printf("buffer flush:%+v\n", b)
}

//this uses the global curworkernum and atomically increases it and returns it
//to the worker. This is a workaround to having workers have only shared cockroachcontext
//and channel.
var (
	curworknum  int
	worknumLock sync.Mutex
)

func getWorkerNum() int {
	worknumLock.Lock()
	defer worknumLock.Unlock()
	curworknum++
	ret := curworknum
	return ret
}

//write buffers messages that arrive on wchan and writes them if they hit a number.
//at the same time it has a ticker that checks if it's getting messages from the cli.
//if the ticker detects that it didn't get any messages it fluses the queue.
//when wchan closes it will detect it and close the ticket too so the goroutine can die
func Write(cc *cockroachContext, wchan <-chan *pb.WriteRequest) {
	var (
		upbuf *buffer
	)
	wnum := getWorkerNum()
	log.Debl.Printf("[worker %d] starting", wnum)
	ticker := time.NewTicker(10 * time.Second)
	idleticks := 0
	obufs := NewOpenBuffers(cc)
	//prefbuf := newbuffer(cc.stmtprstr, 3000, cc, true) // fits around 300 objects include a default first arg in stmt
	for {
		select {
		case request, wchopen := <-wchan:
			//check if someone closed our chan and signal the ticker
			if !wchopen {
				fmt.Printf("chan closed. calling flush\n")
				upbuf.flush(true)
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
			opbuf, found, err := obufs.Find(fmt.Sprintf("%s", colip), timestamp)
			if !found {
				log.Errl.Print("open buffer neither found nor created. Error:%s", err)
				continue
			}
			upbuf = opbuf.buf
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
				nhip net.IP
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
			upbuf.add(id, timestamp, []byte(colip), []byte(peerip), aspstr, []byte(nhip), lastas, capbytes)
			/*row := cc.stmupdate.QueryRow(
				timestamp, []byte(colip), colipstr, []byte(peerip), peeripstr, aspstr, []byte(nhip), nhipstr, capbytes)
			if errid := row.Scan(&id); errid != nil {
				log.Errl.Printf("error in fetching id from last insert:%s", errid)
				continue
			}*/
			/*
				if msgUp.WithdrawnRoutes != nil && len(msgUp.WithdrawnRoutes.Prefixes) != 0 {
					for _, wr := range msgUp.WithdrawnRoutes.Prefixes {
						ip, _, err := parseIpToIPString(*wr.Prefix)
						if err != nil {
							log.Errl.Printf("error:%s parsing withdrawn prefix", err)
							continue
						}
						mask := int(wr.Mask)
						///XXX hardcoded table
						//prefbuf.add(id, []byte(ip), mask, lastas, timestamp, true)
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
						//prefbuf.add(id, []byte(ip), mask, lastas, timestamp, false)
					}
				}
			*/
		case <-ticker.C:
			idleticks++
			if idleticks > 1 { // 10 or more seconds passed since a msg arrived. flush
				if upbuf != nil {
					log.Debl.Printf("[worker %d] flushing due to inacivity\n", wnum)
					upbuf.flush(true)
				}
				//prefbuf.flush(true)
				idleticks = 0
			}
		}
	}
	log.Debl.Printf("[worker %d] exiting", wnum)

	return
}

func (b BGPCapture) WriteCon(cc *cockroachContext, request *pb.WriteRequest) error {
	b.cc = cc
	return b.Write(request)
}
