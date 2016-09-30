package bgp

import (
	"errors"
    "fmt"
	//"net"
	"time"

	"github.com/CSUNetSec/bgpmon/log"
	"github.com/CSUNetSec/bgpmon/module"
	"github.com/CSUNetSec/bgpmon/session"
)

const (
    //prefixByAsNumberStmt = "SELECT prefix_ip_address, prefix_mask, as_number, dateOf(timestamp) FROM %s.as_number_by_prefix_range WHERE time_bucket=? AND prefix_ip_address>=? AND prefix_ip_address<=?"
    asNumberByPrefixRangeStmt = "SELECT prefix_ip_address, prefix_mask, timestamp, as_number FROM %s.as_number_by_prefix_range WHERE time_bucket=?"
    prefixByAsNumberStmt = "INSERT INTO %s.prefix_by_as_number(as_number, measure_date, prefix_ip_address, prefix_mask, update_message_count) VALUES(?, ?, ?, ?, ?)"
)

//struct for use in parsing bgpmond toml configuration file
type PrefixByAsNumberConfig struct {
	Keyspaces       []string
    WriteKeyspace   string
}

type PrefixByAsNumberModule struct {
    startTime       int64
    endTime         int64
	inSessions      []session.CassandraSession
	keyspaces       []string
    writeKeyspace   string
	status          *PrefixByAsNumberStatus
}

type PrefixByAsNumberStatus struct {
    prefixCount     int64
}

func NewPrefixByAsNumberModule(startTime int64, endTime int64, inSessions []session.Sessioner, config PrefixByAsNumberConfig) (*module.Module, error) {
	//check that all sessions are cassandra sessions
	inSess := []session.CassandraSession{}
	for _, sess := range inSessions {
		casSess, ok := sess.(session.CassandraSession)
		if !ok {
			return nil, errors.New("Only cassandra sessions are supported for prefix hijack module")
		}

		inSess = append(inSess, casSess)
	}

	return &module.Module{Moduler:PrefixByAsNumberModule{startTime, endTime, inSess, config.Keyspaces, config.WriteKeyspace, &PrefixByAsNumberStatus{0}}}, nil
}

func (p PrefixByAsNumberModule) Run() error {
    log.Debl.Printf("Running prefix by as number module with start time:%d and end time:%d\n", p.startTime, p.endTime)

    //parse start and end times
    startTime := time.Unix(p.startTime, 0)
    endTime := time.Unix(p.endTime, 0)

    //startDate := time.Unix(p.startTime - (p.startTime % 86400), 0)
    //endDate := time.Unix(p.endTime - (p.endTime % 86400), 0)

    //determine time buckets to be queried
	timeBuckets, err := getTimeBuckets(startTime, endTime)
	if err != nil {
		return err
	}

    //loop over time buckets
    var (
        ipAddress, currentIpAddress string
        mask, currentMask, asNumber, currentAsNumber, count int
        timestamp time.Time
    )

    for _, timeBucket := range timeBuckets {
        for _, session := range p.inSessions {
            for _, keyspace := range p.keyspaces {
                query := session.CqlSession.Query (
                    fmt.Sprintf(asNumberByPrefixRangeStmt, keyspace),
                    timeBucket,
                )

                err := query.Exec()
                if err != nil {
                    panic(err)
                }

                iter := query.Iter()
                for iter.Scan(&ipAddress, &mask, &timestamp, &asNumber) {
                    if timestamp.Before(startTime) || timestamp.After(endTime) {
                        continue
                    }

                    if ipAddress != currentIpAddress || mask != currentMask || asNumber != currentAsNumber {
                        if count != 0 {
                            fmt.Printf("%d %v:%d\n", currentAsNumber, currentIpAddress, currentMask)
                            err := session.CqlSession.Query (
                                fmt.Sprintf(prefixByAsNumberStmt, p.writeKeyspace),
                                currentAsNumber,
                                timeBucket,
                                currentIpAddress,
                                currentMask,
                                count,
                            ).Exec()

                            if err != nil {
                                return err
                            }
                        }

                        currentIpAddress = ipAddress
                        currentMask = mask
                        currentAsNumber = asNumber
                        count = 1
                    } else {
                        count++
                    }
                }

                err = session.CqlSession.Query (
                    fmt.Sprintf(prefixByAsNumberStmt, p.writeKeyspace),
                    currentAsNumber,
                    timeBucket,
                    currentIpAddress,
                    currentMask,
                    count,
                ).Exec()

                if err != nil {
                    return err
                }
            }
        }
	}

    fmt.Printf("done\n")
	return nil
}

func (p PrefixByAsNumberModule) Status() string {
	return ""
}

func (p PrefixByAsNumberModule) Cleanup() error {
	return nil
}
