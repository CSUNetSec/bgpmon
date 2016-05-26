package bgp

import (
	"errors"
    "fmt"
	"net"
	"time"

	"github.com/CSUNetSec/bgpmon/log"
	"github.com/CSUNetSec/bgpmon/module"
	"github.com/CSUNetSec/bgpmon/session"
)

const (
    asNumberByPrefixStmt = "SELECT prefix_ip_address, prefix_mask, as_number, dateOf(timestamp) FROM %s.as_number_by_prefix_range WHERE time_bucket=? AND prefix_ip_address>=? AND prefix_ip_address<=?"
)

//struct for use in parsing bgpmond toml configuration file
type PrefixHijackConfig struct {
	Keyspaces []string
}

type PrefixHijackModule struct {
	prefixIPAddress net.IP
	prefixMask      int
	asNumbers       []uint32
	periodicSeconds int32
	timeoutSeconds  int32
	inSessions      []session.CassandraSession
	keyspaces       []string
	status          *PrefixHijackStatus
}

type PrefixHijackStatus struct {
	ExecutionCount    uint
	LastExecutionTime time.Time
}

func NewPrefixHijackModule(prefix string, asNumbers []uint32, periodicSeconds, timeoutSeconds int32, inSessions []session.Sessioner, config PrefixHijackConfig) (*module.Module, error) {
	//parse cidr address
	_, ipNet, err := net.ParseCIDR(prefix)
	if err != nil {
		return nil, err
	}

	mask, _ := ipNet.Mask.Size()

	//check that all sessions are cassandra sessions
	inSess := []session.CassandraSession{}
	for _, sess := range inSessions {
		casSess, ok := sess.(session.CassandraSession)
		if !ok {
			return nil, errors.New("Only cassandra sessions are supported for prefix hijack module")
		}

		inSess = append(inSess, casSess)
	}

	return &module.Module{Moduler:PrefixHijackModule{ipNet.IP, mask, asNumbers, periodicSeconds, timeoutSeconds, inSess, config.Keyspaces, &PrefixHijackStatus{0, time.Now()}}}, nil
}

func (p PrefixHijackModule) Run() error {
	log.Debl.Printf("Running prefix hijack module\n")

	//set detection start and end time
	endTime := time.Now().UTC()
	startTime := endTime.Add(time.Duration(-1 * p.periodicSeconds) * time.Second)
	if startTime.After(p.status.LastExecutionTime) { //set start time to min(LastExecutionTime, time.Now()-periodicSecs)
		startTime = p.status.LastExecutionTime
	}

	//determine time buckets to be queried
	timeBuckets, err := getTimeBuckets(startTime, endTime)
	if err != nil {
		return err
	}

	//parse min and max ip addresses
	minIPAddress, maxIPAddress, err := getIPRange(p.prefixIPAddress, p.prefixMask)
	if err != nil {
		return err
	}

	//loop through time buckets
    var (
        ipAddress string
        mask, asNumber int
        timestamp time.Time
    )

	for _, timeBucket := range timeBuckets {
        fmt.Printf("querying timebucket %v\n", timeBucket)
        for _, session := range p.inSessions {
            for _, keyspace := range p.keyspaces {
                fmt.Printf("querying keyspace %s %v %v\n", keyspace, minIPAddress, maxIPAddress)
                query := session.CqlSession.Query(
                    fmt.Sprintf(asNumberByPrefixStmt, keyspace),
                    timeBucket,
                    minIPAddress,
                    maxIPAddress)

                iter := query.Iter()
                for iter.Scan(&ipAddress, &mask, &asNumber, &timestamp) {
                    if mask < p.prefixMask || timestamp.Before(startTime) || timestamp.After(endTime) || intContains(p.asNumbers, uint32(asNumber)) {
                        continue
                    }

                    fmt.Printf("NOTIFICATION OF HIJACK - TIMESTAMP:%v IP_ADDRESS:%s MASK:%d AS_NUMBER:%d\n", timestamp, ipAddress, mask, asNumber)
                }
            }
        }
	}

	//update status variables
	p.status.ExecutionCount++
	p.status.LastExecutionTime = endTime
	return nil
}

func (p PrefixHijackModule) Status() string {
	return ""
}

func (p PrefixHijackModule) Cleanup() error {
	return nil
}

func intContains(list []uint32, value uint32) bool {
    for _, num := range list {
        if value == num {
            return true
        }
    }

    return false
}

/*import (
	"encoding/json"
	"net"
	"time"

	"github.com/CSUNetSec/bgpmon/log"
	"github.com/CSUNetSec/bgpmon/module"
	"github.com/CSUNetSec/bgpmon/protocol"
	"github.com/CSUNetSec/bgpmon/session"

	"github.com/gocql/gocql"
)

type PrefixHijackModule struct {
	module.Module
	options *PrefixHijackOptions
	status  *PrefixHijackStatus
	session *gocql.Session
}

type PrefixHijackOptions struct {
	PrefixIPAddress net.IP `json:"PrefixIPAddress"`
	PrefixMask      int    `json:"PrefixMask"`
	ASNumbers       []int  `json:"ASNumbers"`
}

type PrefixHijackStatus struct {
	ExecutionCount     uint      `json:"ExecutionCount"`
	LastExecution      time.Time `json:"LastExecution"`
	Hijacks            []Hijack  `json:"Hijacks"`
}

type Hijack struct {
	PrefixIPAddress    string               `json:"PrefixIPAddress"`
	PrefixMask         int                  `json:"PrefixMask"`
	PeerIPAddress      string               `json:"PeerIPAddress"`
	PeerLocation       []protocol.Location  `json:"PeerLocations"`
	CollectorIPAddress string               `json:"CollectorIPAddress"`
	CollectorLocation  []protocol.Location  `json:"CollectorLocations"`
	ASPath             []AS                 `json:"ASPath"`
	Timestamp          time.Time            `json:"Timestamp"`
}

type AS struct {
	ASNumber  int                 `json:"ASNumber"`
	Locations []protocol.Location `json:"Locations"`
}

func NewPrefixHijackModule(conf string, ios session.IOSessions) module.Moduler {
	//check if in session is there cause this mod needs one in session
	if len(ios.In) != 1 {
		return nil
	}
	session := ios.In[0].GetCasSession()

	ret := &PrefixHijackModule{module.NewModule(conf), new(PrefixHijackOptions), &PrefixHijackStatus{0, time.Now().UTC(), []Hijack{}}, session}

	//parse configuration string
	if err := ret.ParseTimerConfig(conf); err != nil {
		log.Errl.Printf("NewPrefixHijackModule error in parsing config:%s\n", err)
		return nil
	}

	if err := json.Unmarshal([]byte(conf), &ret.options); err != nil {
		log.Errl.Printf("NewPrefixHijackModule error in parsing options:%s\n", err)
		return nil
	}

	return ret
}

func (p *PrefixHijackModule) Run() error {
	//create a start and end time
	endTime := time.Now().UTC()
	startTime := p.status.LastExecution
	//startTime := endTime.Add(time.Duration(-1 * p.Module.Timers.PeriodicSecs) * time.Second)

	startDate := time.Unix(startTime.Unix()-(startTime.Unix()%86400), 0)
	endDate := time.Unix(endTime.Unix()-(endTime.Unix()%86400), 0)

	dates := []time.Time{startDate}
	if !startDate.Equal(endDate) {
		dates = append(dates, endDate)
	}

	//determine ip address range
	minIPAddress, maxIPAddress := getIPRange(p.options.PrefixIPAddress, p.options.PrefixMask)

	//issue query
	var ipAddress, timeuuid string
	var mask, asNumber int
	var timestamp time.Time
	for _, date := range dates {
		query := p.session.Query(
			"SELECT prefix_ip_address, prefix_mask, as_number, timestamp, dateOf(timestamp) FROM csu_bgp_derived.as_number_by_prefix_range WHERE time_bucket=? AND prefix_ip_address>= ? AND prefix_ip_address<=?",
			date,
			minIPAddress,
			maxIPAddress)

		iter := query.Iter()
		for iter.Scan(&ipAddress, &mask, &asNumber, &timeuuid, &timestamp) {
			if mask < p.options.PrefixMask || timestamp.Before(startTime) || timestamp.After(endTime) || contains(p.options.ASNumbers, asNumber) {
				continue
			}

			//get as path of message
			updateMsgQuery := p.session.Query("SELECT as_path, peer_ip_address, collector_ip_address FROM csu_bgp_core.update_messages_by_time WHERE time_bucket=? AND timestamp=?", date, timeuuid)
			updateMsgIter := updateMsgQuery.Iter()
			var asPathValues []int
			var peerIPAddress, collectorIPAddress string
			for updateMsgIter.Scan(&asPathValues, &peerIPAddress, &collectorIPAddress) {
			}

			log.Debl.Printf("NOTIFICATION OF HIJACK for prefix:%v/%v at timestamp '%v' with as_path:%v\n", ipAddress, mask, timestamp, asPathValues)

			//geolocation lookup on AS path
			asPath := []AS{}
			for _, asNumber := range asPathValues {
				locations := protocol.GetLocationByASNumber(asNumber, p.session)
				asPath = append(asPath, AS{ASNumber: asNumber, Locations: locations})
			}

			//geolocate peer and collector ip address
			peerLocations := protocol.GetLocationByIPAddress(peerIPAddress, p.session)
			collectorLocations := protocol.GetLocationByIPAddress(collectorIPAddress, p.session)

			hijack := Hijack {
				PrefixIPAddress: ipAddress,
				PrefixMask: mask,
				PeerIPAddress: peerIPAddress,
				PeerLocation: peerLocations,
				CollectorIPAddress: collectorIPAddress,
				CollectorLocation: collectorLocations,
				ASPath: asPath,
				Timestamp: timestamp,
			}
			p.status.Hijacks = append(p.status.Hijacks, hijack)

			//jsonHijack, _ := json.Marshal(hijack)
			//log.Debl.Printf("%s\n", jsonHijack)
			//log.Debl.Printf("http:///home/hamersaw/development/go/src/bgpmon/util/webfrontent/asgooglemap.html\n")
		}
	}

	hijacks := []Hijack{}
	for _, hijack := range p.status.Hijacks {
		if time.Since(hijack.Timestamp).Hours() > 24 {
			hijacks = append(hijacks, hijack)
		}
	}

	p.status.Hijacks = hijacks
	p.status.ExecutionCount++
	p.status.LastExecution = endTime
	return nil
}

func (p *PrefixHijackModule) Status() string {
	str, _ := json.Marshal(p.status)
	return string(str)
}

func (p *PrefixHijackModule) Cleanup() {
	return
}

func getIPRange(ip net.IP, mask int) (string, string) {
	var maskBytes []byte
	if ip.To4() != nil {
		maskBytes = net.CIDRMask(mask, 32)
	} else {
		maskBytes = net.CIDRMask(mask, 128)
	}

	ip = ip.Mask(maskBytes)
	minIPAddress := ip.String()
	for i := 0; i < len(maskBytes); i++ {
		ip[i] = ip[i] | ^maskBytes[i]
	}

	maxIPAddress := ip.String()

	return minIPAddress, maxIPAddress
}

func contains(list []int, value int) bool {
	for _, num := range list {
		if value == num {
			return true
		}
	}

	return false
}*/
