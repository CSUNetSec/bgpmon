package modules

import (
	"fmt"
	core "github.com/CSUNetSec/bgpmon"
	"github.com/CSUNetSec/bgpmon/util"
	"strings"
	"time"
)

// PeriodicModule will run another module repeatedly until it is cancelled.
type periodicModule struct {
	*BaseDaemon
}

// Run will launch the periodic daemon. Args should specify the duration first,
// then the module to run with any arguments needed to pass to that module
func (p *periodicModule) Run(argStr string, f core.FinishFunc) error {
	args := strings.SplitN(argStr, " ", 3)
	if len(args) != 3 {
		p.logger.Errorf("Expected 3 arguments, got %d", len(args))
		f()
		return nil
	}

	dur, err := time.ParseDuration(args[0])
	if err != nil {
		p.logger.Errorf("Error parsing duration: %s", args[0])
		f()
		return nil
	}
	modName := args[1]
	modArgs := args[2]

	tick := time.NewTicker(dur)
	defer tick.Stop()
	runC := 0
	errC := 0
	for {
		select {
		case <-p.cancel:
			p.logger.Infof("Stopping periodic")
			return nil
		case <-tick.C:
			mID := fmt.Sprintf("periodic-%s%d", modName, runC)
			err = p.server.RunModule(modName, mID, modArgs)
			if err != nil {
				p.logger.Errorf("Error running module(%s): %s", modName, err)
				errC++
			} else {
				errC = 0
			}

			if errC >= 5 {
				p.logger.Errorf("Failed to run module 5 times, stopping.")
				f()
				return nil
			}
		}
		runC++
	}
}

func newPeriodicModule(s core.BgpmondServer, l util.Logger) core.Module {
	return &periodicModule{NewBaseDaemon(s, l, "periodic")}
}

func init() {
	core.RegisterModule("periodic", newPeriodicModule)
}
