package modules

import (
	"fmt"
	"strings"
	"time"

	core "github.com/CSUNetSec/bgpmon"
	"github.com/CSUNetSec/bgpmon/util"
)

// PeriodicModule will run another module repeatedly until it is cancelled.
type periodicModule struct {
	*BaseDaemon
}

// Run will launch the periodic daemon. Args should specify the duration,
// module to run and any arguments needed to pass to that module
// Optkeys should be: duration , module
// All optKeys in the format T* will be passed on to the target module.
// Ex. -Tfoo bar will be passed as -foo bar
func (p *periodicModule) Run(args map[string]string, f core.FinishFunc) {
	defer p.wg.Done()
	// f is not deferred because it should not be run if the modules
	// Close method is called.
	if !util.CheckForKeys(args, "duration", "module") {
		p.logger.Errorf("Expected option keys: duration, module, args. Got %v", args)
		f()
		return
	}

	dval, modval := args["duration"], args["module"]
	argmap := make(map[string]string)
	for k, v := range args {
		// Args that begin with a T are forwarded to the target module
		if strings.HasPrefix(k, "T") {
			// Ignore the T, pass the rest of the arg
			argmap[k[1:]] = v
		}
	}

	dur, err := time.ParseDuration(dval)
	if err != nil {
		p.logger.Errorf("Error parsing duration: %s", dval)
		f()
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
			mID := fmt.Sprintf("periodic-%s%d", modval, runCt)
			err = p.server.RunModule(modval, mID, argmap)
			if err != nil {
				p.logger.Errorf("Error running module(%s): %s", modval, err)
				errCt++
			} else {
				errCt = 0
			}

			if errCt >= 5 {
				p.logger.Errorf("Failed to run module 5 times, stopping.")
				f()
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
			Description: "Continuosly run another module at scheduled intervals",
			Opts:        opts,
		},
		Maker: newPeriodicModule,
	}
	core.RegisterModule(periodicHandle)
}
