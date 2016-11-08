package bgp

import (
    "bytes"
    "errors"
    "fmt"
    "net"
    "time"

	"github.com/CSUNetSec/bgpmon/log"
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

func NewPrefixHijackModule(monitorPrefixes []*pbbgpmon.PrefixHijackModule_MonitorPrefix, periodicSeconds, timeoutSeconds int32, inSessions []session.Sessioner, config PrefixHijackConfig) (*module.Module, error) {
	//check that all sessions are cassandra sessions
	inSess := []session.CassandraSession{}
	for _, sess := range inSessions {
		casSess, ok := sess.(session.CassandraSession)
		if !ok {
			return nil, errors.New("Only cassandra sessions are supported for prefix hijack module")
		}

		inSess = append(inSess, casSess)
	}

    //populate prefix cache
    prefixCache := NewPrefixCache()
    for _, monitorPrefix := range monitorPrefixes {
        prefixCache.AddPrefix(monitorPrefix.Prefix.Prefix.Ipv4, monitorPrefix.Prefix.Mask, monitorPrefix.AsNumber)
    }

    //prefixCache.Print()

	return &module.Module{Moduler: PrefixHijackModule{prefixCache, periodicSeconds, timeoutSeconds, inSess, config.Keyspaces, &PrefixHijackStatus{0, time.Now()}}}, nil
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

	/*//parse min and max ip addresses
	minIPAddress, maxIPAddress, err := getIPRange(p.prefixIPAddress, p.prefixMask)
	if err != nil {
		return err
	}*/

	//loop through time buckets
	var (
		ipAddress      string
		mask, asNumber uint32
		timestamp      time.Time
	)

	for _, timeBucket := range timeBuckets {
		for _, session := range p.inSessions {
			for _, keyspace := range p.keyspaces {
                for _, prefixNode := range p.prefixCache.prefixNodes {
                    query := session.CqlSession.Query(
                        fmt.Sprintf(asNumberByPrefixStmt, keyspace),
                        timeBucket,
                        prefixNode.minAddress,
                        prefixNode.maxAddress)


                    iter := query.Iter()
                    for iter.Scan(&ipAddress, &mask, &asNumber, &timestamp) {
                        //check for valid mask, timestamp, and if source is a valid asNumber
                        if mask < prefixNode.mask || timestamp.Before(startTime) || timestamp.After(endTime) || intContains(prefixNode.asNumbers, uint32(asNumber)) {
                            continue
                        }

                        //check if parent nodes contain as number
                        node := prefixNode
                        found := false
                        for node.parent != nil {
                            node = node.parent

                            if intContains(node.asNumbers, uint32(asNumber)) {
                                found = true
                                break
                            }
                        }

                        if found {
                            continue
                        }

                        /*//TODO retrieve as path of message - query update_messages_by_time with timeuuid
                        updateMsgQuery := p.session.Query("SELECT as_path, peer_ip_address, collector_ip_address FROM csu_bgp_core.update_messages_by_time WHERE time_bucket=? AND timestamp=?", date, timeuuid)
                        updateMsgIter := updateMsgQuery.Iter()
                        var asPathValues []int
                        var peerIPAddress, collectorIPAddress string
                        for updateMsgIter.Scan(&asPathValues, &peerIPAddress, &collectorIPAddress) {
                        }*/

                        //TODO check historical data by querying prefix_by_as_number

                        fmt.Printf("NOTIFICATION OF HIJACK - TIMESTAMP:%v IP_ADDRESS:%s MASK:%d AS_PATH:%d\n", timestamp, ipAddress, mask, asNumber)
                    }
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
