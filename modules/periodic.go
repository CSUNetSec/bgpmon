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
	runC := 0
	for {
		select {
		case <-p.cancel:
			tick.Stop()
			p.logger.Infof("Stopping periodic")
			return nil
		case <-tick.C:
			mID := fmt.Sprintf("periodic-%s%d", args[1], runC)
			err = p.server.RunModule(modName, mID, modArgs)
			if err != nil {
				p.logger.Errorf("Error running module(%s): %s", modName, err)
			}
		}
		runC++
	}
}

// GetName returns the module type name, "periodic"
func (p *periodicModule) GetName() string {
	return "periodic"
}

func newPeriodicModule(s core.BgpmondServer, l util.Logger) core.Module {
	return &periodicModule{NewBaseDaemon(s, l, "periodic")}
}

func init() {
	core.RegisterModule("periodic", newPeriodicModule)
}
