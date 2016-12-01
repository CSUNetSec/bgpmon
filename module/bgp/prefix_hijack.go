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
    monitorPrefixesStmt = "SELECT as_number, enabled, ip_address, mask FROM monitor_prefixes WHERE module_id = $1 AND enabled = true"
    prefixesStmt        = "SELECT prefix_id, update_id, ip_adddress, mask, source_as FROM prefixes WHERE ip_address > $1 AND ip_address < $2 AND mask >= $3 AND timestamp < $4 AND timestamp > $5 AND is_withdrawal = false"

	//asNumberByPrefixStmt    = "SELECT timestamp, dateOf(timestamp), prefix_ip_address, prefix_mask, as_number, is_withdrawal FROM %s.as_number_by_prefix_range WHERE time_bucket=? AND prefix_ip_address>=? AND prefix_ip_address<=?"
    //monitorPrefixesStmt     = "SELECT as_number, enabled, ip_address, mask FROM csu_bgp_config.monitor_prefixes WHERE module_id = ?"
	//updateMessageSelectStmt = "SELECT as_path, peer_ip_address, collector_ip_address FROM csu_bgp_core.update_messages_by_time WHERE time_bucket=? AND timestamp=?"
	//prefixHijacksStmt       = "INSERT INTO csu_bgp_derived.prefix_hijacks(time_bucket, module_id, timestamp, advertised_ip_address, advertised_mask, monitor_ip_address, monitor_mask) VALUES(?,?,?,?,?,?,?)"
)

//struct for use in parsing bgpmond toml configuration file
type PrefixHijackConfig struct {
	Keyspaces []string
}

type PrefixHijackModule struct {
    moduleId        string
	periodicSeconds int32
	timeoutSeconds  int32
	inSessions      []session.CockroachSession
	keyspaces       []string
	status          *PrefixHijackStatus
    hijackIds       map[string]int64
}

type PrefixHijackStatus struct {
	ExecutionCount    uint
	LastExecutionTime time.Time
}

func NewPrefixHijackModule(moduleId string, periodicSeconds, timeoutSeconds int32, inSessions []session.Sessioner, config PrefixHijackConfig) (*module.Module, error) {
	//check that all sessions are cockroach sessions
	inSess := []session.CockroachSession{}
	for _, sess := range inSessions {
		casSess, ok := sess.(session.CockroachSession)
		if !ok {
			return nil, errors.New("Only cockroach sessions are supported for prefix hijack module")
		}

		inSess = append(inSess, casSess)
	}

	return &module.Module{Moduler: PrefixHijackModule{moduleId, periodicSeconds, timeoutSeconds, inSess, config.Keyspaces, &PrefixHijackStatus{0, time.Now()}, make(map[string]int64)}}, nil
}

func (p PrefixHijackModule) Run() error {
	log.Debl.Printf("Running prefix hijack module\n")

    //get execution time
	executionTime := time.Now().UTC()

    var (
        ipAddress          net.IP
        mask, asNumber     uint32
        enabled            bool

        prefixId, updateId string
    )

    //loop over sessions
    for _, session := range p.inSessions {
        //connect to database
        db, err := session.GetDbConnection()
        if err != nil {
            log.Errl.Printf("Unable to get db connection for prefix hijack: %s", err)
            continue
        }

        rows, err := db.Query(monitorPrefixesStmt, p.moduleId)
        if err != nil {
            log.Errl.Printf("Failed to retrieve monitor prefixes: %s", err)
            continue
        }

        //populate prefix cache
        prefixCache := NewPrefixCache()
        for rows.Next() {
            if rows.Err() != nil {
                log.Errl.Printf("Failed to retrieve next row for monitor prefix: %s", rows.Err())
                continue
            }

            err := rows.Scan(&asNumber, &enabled, &ipAddress, &mask)
            if err != nil {
                log.Errl.Printf("Failed to parse fields for montior prefix: %s", err)
            }

            prefixCache.AddPrefix(ipAddress, mask, asNumber)
        }

        //check each prefix node for a hijack
        for _, prefixNode := range prefixCache.prefixNodes {
			log.Debl.Printf("CHECKING FOR HIJACKS ON %s/%d\n", prefixNode.ipAddress, prefixNode.mask)

            //TODO decrement min time
            //query for potential hijacks
            rows, err := db.Query(prefixesStmt, prefixNode.minAddress, prefixNode.maxAddress, prefixNode.mask, executionTime, executionTime)
            if err != nil {
                log.Errl.Printf("")
                continue
            }

            for rows.Next() {
                if rows.Err() != nil {
                    log.Errl.Printf("Failed to retrieve row for potential hijack: %s", err)
                    continue
                }

                err := rows.Scan(&prefixId, &updateId, &ipAddress, &mask, &asNumber)
                if err != nil {
                    log.Errl.Printf("Failed to parse fields on potential hijack: %s", err)
                }

                //check if potential hijack has already been seen
                if _, ok := p.hijackIds[prefixId]; ok {
                    continue
                }

                //TODO retrieve as path of message - query update_messages_by_time with timeuuid

                //TODO check historical data

                //fmt.Printf("\tNOTIFICATION OF HIJACK - TIMESTAMP:%v IP_ADDRESS:%s MASK:%d AS_PATH:%d\n", ipAddress, mask, asNumber)
                log.Debl.Printf("\tNOTIFICATION OF HIJACK - IP_ADDRESS:%s MASK:%d AS_NUMBER:%d\n", ipAddress, mask, asNumber)
                p.hijackIds[prefixId] = time.Now().Unix()

                //TODO write hijack to cockroach
            }
        }

        //close db
        db.Close()
    }

	/*var (
		timeuuid                          string
		timestamp                         time.Time
		ipAddress                         net.IP
		mask, asNumber                    uint32
		enabled, isWithdrawal             bool

		asPath                            []int
		peerIpAddress, collectorIpAddress string
	)
    //populate prefix cache
    prefixCache := NewPrefixCache()
    for _, session := range p.inSessions {
        monitorPrefixesIter := session.CqlSession.Query(monitorPrefixesStmt, p.moduleId).Iter()
        for monitorPrefixesIter.Scan(&asNumber, &enabled, &ipAddress, &mask) {
            if !enabled {
                continue
            }

            prefixCache.AddPrefix(ipAddress, mask, asNumber)
        }
    }

	//get execution time and initialize timebuckets to today and yesterday
	executionTime := time.Now().UTC()
	timeBuckets := []time.Time{getTimeBucket(executionTime), getTimeBucket(time.Unix(executionTime.Unix()-86400, 0))}

	//loop through time buckets
	for _, timeBucket := range timeBuckets {
		for _, session := range p.inSessions {
			for _, keyspace := range p.keyspaces {
				for _, prefixNode := range prefixCache.prefixNodes {
					//fmt.Printf("CHECKING FOR HIJACKS ON %s/%d\n", prefixNode.ipAddress, prefixNode.mask)
					prefixRangeIter := session.CqlSession.Query(fmt.Sprintf(asNumberByPrefixStmt, keyspace), timeBucket, net.IP(prefixNode.minAddress), net.IP(prefixNode.maxAddress)).Iter()
					for prefixRangeIter.Scan(&timeuuid, &timestamp, &ipAddress, &mask, &asNumber, &isWithdrawal) {
						//make sure the message is an advertisement and not withdrawl
						if isWithdrawal {
							continue
						}

						//check for valid mask, timestamp, and if source is a valid asNumber
						if mask < prefixNode.mask {
							continue
						}

						//check if potential hijack has already been seen
                        aggregateString := fmt.Sprintf("%s-%s/%d->%s/%d", timeuuid, ipAddress, mask, prefixNode.ipAddress, prefixNode.mask)
                        if _, ok := p.hijackUUIDs[aggregateString]; ok {
                            continue
                        }

						//retrieve as path of message - query update_messages_by_time with timeuuid
						updateMessageIter := session.CqlSession.Query(updateMessageSelectStmt, timeBucket, timeuuid).Iter()
						if updateMessageIter.Scan(&asPath, &peerIpAddress, &collectorIpAddress) {
							found := false
							for _, asNum := range asPath {
								if !prefixNode.ValidAsNumber(uint32(asNum)) {
									found = true
									break
								}
							}

							if found {
								continue
							}
						} else {
							//if message not found only check the source as on as_numbers_by_prefix_range
							if !prefixNode.ValidAsNumber(uint32(asNumber)) {
								continue
							}
						}

						//TODO check historical data by querying prefix_by_as_number

						fmt.Printf("\tNOTIFICATION OF HIJACK - TIMESTAMP:%v IP_ADDRESS:%s MASK:%d AS_PATH:%d\n", timestamp, ipAddress, mask, asNumber)
                        p.hijackUUIDs[aggregateString] = time.Now().Unix()

						//write hijack to cassandra
						err := session.CqlSession.Query(prefixHijacksStmt, timeBucket, p.moduleId, timeuuid, ipAddress, mask, prefixNode.ipAddress, prefixNode.mask).Exec()
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}*/

    //TODO remove prefix hijack UUIDs that are older than 2 days

	//update status variables
	p.status.ExecutionCount++
	p.status.LastExecutionTime = executionTime
	return nil
}

func (p PrefixHijackModule) Status() string {
	return fmt.Sprintf("%v", p.status)
}

func (p PrefixHijackModule) Cleanup() error {
	return nil
}
