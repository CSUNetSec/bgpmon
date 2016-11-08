package bgp

import (
    "bytes"
    "fmt"
    "net"
    "time"

	"github.com/CSUNetSec/bgpmon/module"
	"github.com/CSUNetSec/bgpmon/session"

	pbbgpmon "github.com/CSUNetSec/netsec-protobufs/bgpmon"
)

const (
	asNumberByPrefixStmt = "SELECT prefix_ip_address, prefix_mask, as_number, dateOf(timestamp) FROM %s.as_number_by_prefix_range WHERE time_bucket=? AND prefix_ip_address>=? AND prefix_ip_address<=?"
)

//struct for use in parsing bgpmond toml configuration file
type PrefixHijackConfig struct {
	Keyspaces []string
}

type PrefixHijackModule struct {
    prefixCache *PrefixCache
}

type PrefixHijackStatus struct {
	ExecutionCount    uint
	LastExecutionTime time.Time
}

func NewPrefixHijackModule(monitorPrefixes []*pbbgpmon.PrefixHijackModule_MonitorPrefix, periodicSeconds, timeoutSeconds int32, inSessions []session.Sessioner, config PrefixHijackConfig) (*module.Module, error) {
    //populate prefix cache
    prefixCache := NewPrefixCache()
    for _, monitorPrefix := range monitorPrefixes {
        prefixCache.AddPrefix(monitorPrefix.Prefix.Prefix.Ipv4, monitorPrefix.Prefix.Mask, monitorPrefix.AsNumber)
    }

    //prefixCache.Print()

    return &module.Module{Moduler: PrefixHijackModule{prefixCache: prefixCache}}, nil
}

func (p PrefixHijackModule) Run() error {
    return nil
}

func (p PrefixHijackModule) Status() string {
	return ""
}

func (p PrefixHijackModule) Cleanup() error {
	return nil
}

/*
 * PrefixCache
 */
type PrefixCache struct {
    roots []*PrefixNode
    prefixNodes []*PrefixNode
}

func NewPrefixCache() *PrefixCache {
    return &PrefixCache {
        roots: []*PrefixNode{},
        prefixNodes: []*PrefixNode{},
    }
}

func (p *PrefixCache) AddPrefix(ipAddress net.IP, mask uint32, asNumbers []uint32) error {
    //create PrefxNode
    prefixNode := NewPrefixNode(&ipAddress, mask, asNumbers)
    p.prefixNodes = append(p.prefixNodes, prefixNode)

    //check if prefixNode is subprefix/superprefix of a root
    removeIndex := -1
    for i, node := range p.roots {
        if prefixNode.SubPrefix(node) {
            //find correct node to insert on
            insertNode := node
            found := true
            for found {
                found = false
                for _, child := range insertNode.children {
                    if prefixNode.SubPrefix(child) {
                        insertNode = child
                        found = true
                    }
                }
            }

            //check if it's a superprefix to any children
            superPrefixIndex := -1
            for i, child := range insertNode.children {
                if prefixNode.SuperPrefix(child) {
                    superPrefixIndex = i
                    break
                }
            }

            if superPrefixIndex != -1 {
                prefixNode.parent = insertNode
                insertNode.children[superPrefixIndex].parent = prefixNode

                prefixNode.children = append(prefixNode.children, insertNode.children[superPrefixIndex])
                insertNode.children = append(insertNode.children[:superPrefixIndex], insertNode.children[superPrefixIndex+1:]...)
            } else {
                prefixNode.parent = insertNode
                insertNode.children = append(insertNode.children, prefixNode)
            }

            return nil
        } else if prefixNode.SuperPrefix(node) {
            //add prefixNode as superprefix to node
            node.parent = prefixNode
            prefixNode.children = append(prefixNode.children, node)

            removeIndex = i
            break
        }
    }

    if removeIndex != -1 {
        //remove value
        p.roots = append(p.roots[:removeIndex], p.roots[removeIndex+1:]...)
    }

    p.roots = append(p.roots, prefixNode)
    return nil
}

func (p *PrefixCache) Print() {
    for _, root := range p.roots {
        root.Print(0)
    }
}

type PrefixNode struct {
    ipAddress *net.IP
    mask uint32
    asNumbers []uint32
    minAddress, maxAddress []byte
    parent *PrefixNode
    children []*PrefixNode
}

func NewPrefixNode(ipAddress *net.IP, mask uint32, asNumbers[]uint32) *PrefixNode {
    minAddress, maxAddress, _ := getIPRange(*ipAddress, int(mask))

    return &PrefixNode {
        ipAddress: ipAddress,
        mask: mask,
        asNumbers: asNumbers,
        minAddress: minAddress,
        maxAddress: maxAddress,
        parent: nil,
        children: []*PrefixNode{},
    }
}

func (p *PrefixNode) SubPrefix(prefixNode *PrefixNode) bool {
    //check if p.mask is shorter
    if p.mask <= prefixNode.mask {
        return false
    }

    //check if p.minAddress < prefixNode.minAddress or p.maxAddress > prefixNode.maxAddress
    if bytes.Compare(p.minAddress, prefixNode.minAddress) < 0 || bytes.Compare(p.maxAddress, prefixNode.maxAddress) > 0 {
        return false
    }

    return true
}

func (p *PrefixNode) SuperPrefix(prefixNode *PrefixNode) bool {
    //check if p.mask is longer
    if p.mask >= prefixNode.mask {
        return false
    }

    //check if p.minAddress > prefixNode.minAddress or p.maxAddress < prefixNode.maxAddress
    if bytes.Compare(p.minAddress, prefixNode.minAddress) > 0 || bytes.Compare(p.maxAddress, prefixNode.maxAddress) < 0 {
        return false
    }

    return true
}

func (p *PrefixNode) Print(indent int) {
    for i := 0; i<indent; i++ {
        fmt.Printf("\t")
    }

    fmt.Printf("%s/%d : %v\n", p.ipAddress, p.mask, p.asNumbers)

    for _, child := range p.children {
        child.Print(indent + 1)
    }
}

/*import (
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

	return &module.Module{Moduler: PrefixHijackModule{ipNet.IP, mask, asNumbers, periodicSeconds, timeoutSeconds, inSess, config.Keyspaces, &PrefixHijackStatus{0, time.Now()}}}, nil
}

func (p PrefixHijackModule) Run() error {
	log.Debl.Printf("Running prefix hijack module\n")

	//set detection start and end time
	endTime := time.Now().UTC()
	startTime := endTime.Add(time.Duration(-1*p.periodicSeconds) * time.Second)
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
		ipAddress      string
		mask, asNumber int
		timestamp      time.Time
	)

	for _, timeBucket := range timeBuckets {
		for _, session := range p.inSessions {
			for _, keyspace := range p.keyspaces {
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

					fmt.Printf("NOTIFICATION OF HIJACK - TIMESTAMP:%v IP_ADDRESS:%s MASK:%d AS_PATH:%d\n", timestamp, ipAddress, mask, asNumber)
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
}*/





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
