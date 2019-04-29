package modules

import (
	"fmt"
	"strings"
	"time"

	core "github.com/CSUNetSec/bgpmon"
	"github.com/CSUNetSec/bgpmon/util"
)

// periodicModule will run another module repeatedly until it is cancelled.
type periodicModule struct {
	*BaseDaemon
}

// Run will launch the periodic daemon. args should specify the duration,
// module to run and any arguments needed to pass to that module.
// Optkeys should be: duration , module
// All optKeys in the format T* will be passed on to the target module.
// Ex. -Tfoo bar will be passed as -foo bar
func (p *periodicModule) Run(args map[string]string) {
	defer p.wg.Done()

	if !util.CheckForKeys(args, "duration", "module") {
		p.logger.Errorf("Expected option keys: duration, module, args. Got %v", args)
		return
	}

	durationOpt, targetMod := args["duration"], args["module"]
	targetOpts := make(map[string]string)
	for k, v := range args {
		// Args that begin with a T are forwarded to the target module
		if strings.HasPrefix(k, "T") {
			// Ignore the T, pass the rest of the arg
			targetOpts[k[1:]] = v
		}
	}

	dur, err := time.ParseDuration(durationOpt)
	if err != nil {
		p.logger.Errorf("Error parsing duration: %s", durationOpt)
		return
	}

	tick := time.NewTicker(dur)
	defer tick.Stop()

	runCt, errCt := 0, 0
	for {
		select {
		case <-p.ctx.Done():
			p.logger.Infof("Stopping periodic")
			return
		case <-tick.C:
			mID := fmt.Sprintf("periodic-%s%d", targetMod, runCt)
			err = p.server.RunModule(targetMod, mID, targetOpts)
			if err != nil {
				p.logger.Errorf("Error running module(%s): %s", targetMod, err)
				errCt++
			} else {
				errCt = 0
			}

			if errCt >= 5 {
				p.logger.Errorf("Failed to run module 5 times, stopping.")
				return
			}
		}
		runCt++
	}
}

func newPeriodicModule(s core.BgpmondServer, l util.Logger) core.Module {
	return &periodicModule{NewBaseDaemon(s, l, "periodic")}
}

func init() {
	opts := "duration : how often to run target module\n" +
		"module : target module to run repeatedly\n" +
		"T* : all options prefaced with a T will be forwared to the target module"

	periodicHandle := core.ModuleHandler{
		Info: core.ModuleInfo{
			Type:        "periodic",
			Description: "Continuously run another module at scheduled intervals",
			Opts:        opts,
		},
		Maker: newPeriodicModule,
	}
	core.RegisterModule(periodicHandle)
}
