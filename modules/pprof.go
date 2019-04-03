package modules

import (
	"net/http"
	_ "net/http/pprof" // Needed so the http ListenAndServe includes the profiler

	core "github.com/CSUNetSec/bgpmon"
	"github.com/CSUNetSec/bgpmon/util"
)

type pprofMod struct {
	*BaseDaemon
}

//Run on the pprof module expects one option named "address"
func (p *pprofMod) Run(opts map[string]string, finish core.FinishFunc) error {
	defer finish()
	if !util.CheckForKeys(opts, "address") {
		return p.logger.Errorf("option address not present")
	}
	addr := opts["address"]
	p.logger.Errorf("%s", http.ListenAndServe(addr, nil))
	return nil
}

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
