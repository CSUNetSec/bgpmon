package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon"
	"github.com/golang/protobuf/proto"
	_ "github.com/lib/pq"
	"log"
	"net"
	"time"
)

var (
	host    string
	port    uint
	certdir string
	user    string
	hjstmt  = "SELECT update_id, prefix_id FROM bgpmon.hijacks"
	upstmt  = "SELECT protomsg, as_path, collector_ip, timestamp FROM bgpmon.updates WHERE update_id=$1"
	phjstmt = "SELECT ip_address,mask,source_as,timestamp,update_id FROM bgpmon.prefixes WHERE prefix_id=$1"
)

func init() {
	flag.StringVar(&host, "host", "localhost", "db host to connect to")
	flag.UintVar(&port, "port", 26257, "db port")
	flag.StringVar(&certdir, "certs", ".", "directory containing the certificates")
	flag.StringVar(&user, "user", "root", "db user")
}

type hijack struct {
	Time      time.Time
	Prefix    net.IP
	Mask      int
	Source_as int
	Prefix_id int
	Update_id string
}

type prefix struct {
	Ip   net.IP
	Mask int
}

func (p prefix) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("{\"Prefix\":\"%s/%d\"}", p.Ip, p.Mask)), nil
}

type BGPCapture struct {
	Timestamp          time.Time
	PeerAs             int
	LocalAs            int
	PeerIp             net.IP
	CollectorIp        net.IP
	AsPath             string
	AdvertisedPrefixes []prefix
	CaptureID          string
}

func NewBGPCapture(a *pb.BGPCapture, capid string) BGPCapture {
	ret := BGPCapture{}
	ret.Timestamp = time.Unix(int64(a.GetTimestamp()), 0)
	ret.PeerAs = int(a.GetPeerAs())
	ret.LocalAs = int(a.GetLocalAs())
	ret.PeerIp = net.IP(a.GetPeerIp().GetIpv4())
	ret.CollectorIp = net.IP(a.GetLocalIp().GetIpv4())
	for _, ps := range a.GetUpdate().GetAttrs().GetAsPath() {
		if ps.GetAsSeq() != nil {
			ret.AsPath += fmt.Sprintf(" (%v) ", ps.GetAsSeq())
		} else if ps.GetAsSet() != nil {
			ret.AsPath += fmt.Sprintf(" [%v] ", ps.GetAsSet())
		}
	}
	prs := a.GetUpdate().GetAdvertizedRoutes().GetPrefixes()
	for i := range prs {
		p := prefix{net.IP(prs[i].GetPrefix().GetIpv4()), int(prs[i].GetMask())}
		ret.AdvertisedPrefixes = append(ret.AdvertisedPrefixes, p)
	}
	ret.CaptureID = capid
	return ret
}

func main() {
	flag.Parse()
	hijacks := []hijack{}
	captures := []BGPCapture{}
	hjups := make(map[string]bool)
	hjprx := make(map[int]bool)
	db, err := sql.Open("postgres", fmt.Sprintf("postgresql://%s@%s:%d/?sslmode=verify-full&sslcert=%s/%s.cert&sslrootcert=%s/ca.cert&sslkey=%s/%s.key&statement_timeout=10000&connect_timeout=10",
		user, host, port, certdir, user, certdir, certdir, user))
	if err != nil {
		log.Fatalf("connect to db failed:%s", err)
	}
	defer db.Close()
	hjrows, err := db.Query(hjstmt)
	if err != nil {
		log.Fatalf("error getting hijacks:%s", err)
	}
	for hjrows.Next() {
		var (
			upid string
			pid  int
		)
		if err := hjrows.Scan(&upid, &pid); err != nil {
			log.Fatalf("error scanning hijack vals:%s", err)
		}
		hjups[upid] = true
		hjprx[pid] = true
	}
	log.Printf("found %d hijacked prefixes spanning %d update messages", len(hjprx), len(hjups))
	hjrows.Close()
	for pid := range hjprx {
		var (
			pref      []byte
			mask      int
			source_as int
			timestamp time.Time
			upid      string
		)
		err := db.QueryRow(phjstmt, pid).Scan(&pref, &mask, &source_as, &timestamp, &upid)
		switch {
		case err == sql.ErrNoRows:
			log.Printf("No prefix with ID:%s", pid)
		case err != nil:
			log.Fatal(err)
		default:
			hj := hijack{
				Time:      timestamp,
				Source_as: source_as,
				Prefix:    net.IP(pref),
				Mask:      mask,
				Prefix_id: pid,
				Update_id: upid,
			}
			hijacks = append(hijacks, hj)
		}
	}
	for upid := range hjups {
		var (
			pmsg      []byte
			as_path   string
			colip     []byte
			timestamp time.Time
		)
		err := db.QueryRow(upstmt, upid).Scan(&pmsg, &as_path, &colip, &timestamp)
		switch {
		case err == sql.ErrNoRows:
			log.Printf("No update with ID:%s", upid)
		case err != nil:
			log.Fatal(err)
		default:
			capturemsg := new(pb.BGPCapture)
			err := proto.Unmarshal(pmsg, capturemsg)
			if err != nil {
				log.Printf("couldn't unmarshal BGP capture bytes:%s", err)
			}
			bcap := NewBGPCapture(capturemsg, upid)
			captures = append(captures, bcap)
			if err != nil {
				log.Printf("couldn't marshal BGP capture to string:%s", err)
			}
		}
	}
	fmt.Printf("Total:%d hijacked prefixes in %d update msgs\n", len(hjprx), len(hjups))
	for i := range hijacks {
		js, _ := json.Marshal(hijacks[i])
		fmt.Printf("Hijack: %s\n", js)
	}
	for i := range captures {
		js, _ := json.Marshal(captures[i])
		fmt.Printf("Capture: %s\n", js)
	}
}
