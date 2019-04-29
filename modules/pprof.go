package modules

import (
	"net/http"
	_ "net/http/pprof" // Needed so the http ListenAndServe includes the profiler.

	core "github.com/CSUNetSec/bgpmon"
	"github.com/CSUNetSec/bgpmon/util"
)

type pprofMod struct {
	*BaseDaemon
}

// Run on the pprof module expects one option named "address".
func (p *pprofMod) Run(opts map[string]string) {
	if !util.CheckForKeys(opts, "address") {
		p.logger.Errorf("Option address not present")
		return
	}

	addr := opts["address"]
	p.logger.Errorf("%s", http.ListenAndServe(addr, nil))
}

// There is no way to interrupt the http.ListenAndServe function used in Run,
// so this module can't be stopped with the current implementation.
func (p *pprofMod) Stop() error {
	return p.logger.Errorf("pprof module cannot be stopped")
}

func newpprofModule(s core.BgpmondServer, l util.Logger) core.Module {
	return &pprofMod{NewBaseDaemon(s, l, "pprof")}
}

func init() {
	pprofHandle := core.ModuleHandler{
		Info: core.ModuleInfo{
			Type:        "pprof",
			Description: "Run the go http profiler",
			Opts:        "address: the address to start the profiler on",
		},
		Maker: newpprofModule,
	}
	core.RegisterModule(pprofHandle)
}
