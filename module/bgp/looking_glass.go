package bgp

import (
	"errors"
	"github.com/CSUNetSec/bgpmon/log"
	"github.com/CSUNetSec/bgpmon/module"
	"github.com/CSUNetSec/bgpmon/session"
	pbbgpmon "github.com/CSUNetSec/netsec-protobufs/bgpmon"
	"time"
)

var (
	etime  = errors.New("end time before start time")
	ensess = errors.New("no input sessions for module")
)

type LookingGlassModule struct {
	startTime time.Time
	endTime   time.Time
	prefixes  []string
	peers     []string
	asns      []uint32
	//this is a hack because we can't Get easily messages from the Sessioner interface.
	inSessions []session.CockroachSession
}

func NewLookingGlassModule(inSessions []session.Sessioner, pb pbbgpmon.LookingGlassModule) (*module.Module, error) {
	mod := &LookingGlassModule{}
	ts, te := time.Unix(pb.GetStartTime(), 0), time.Unix(pb.GetEndTime(), 0)
	if te.Before(ts) {
		return nil, etime
	}
	mod.startTime = ts
	mod.endTime = te
	mod.prefixes = pb.GetPrefixes()
	mod.peers = pb.GetPeers()
	mod.asns = pb.GetAsns()
	//for now because we can't easily Get msgs from the Sessioner interface cast the session to the
	//cockroachdb type so we can access the underlying sql.DB
	for _, sess := range inSessions {
		cSess, ok := sess.(session.CockroachSession)
		if !ok {
			return nil, errors.New("Only cockroach sessions are supported for prefix hijack module")
		}

		mod.inSessions = append(mod.inSessions, cSess)
	}

	return &module.Module{Moduler: mod}, nil
}

func (l LookingGlassModule) Cleanup() error {
	return nil
}

func (l LookingGlassModule) Run() error {
	if len(l.inSessions) < 1 {
		return ensess
	}
	// XXX only using the first provided session. maybe have one session only as the Module param
	db := l.inSessions[0].GetRandDbConnection()
	log.Debl.Printf("from session:%+v got a rand db connection of:%+v", l.inSessions[0], db)
	tnames, err := session.GetRelevantTableNames(db, "", l.startTime, l.endTime)
	log.Debl.Printf("got result tablenames:%s error:%s", tnames, err)
	if len(l.asns) > 0 {
		err = session.GetPrefixesForASns(db, l.startTime, l.endTime, tnames, l.asns)
	}
	return nil
}

func (l LookingGlassModule) Status() string {
	return "looking glass running"
}
