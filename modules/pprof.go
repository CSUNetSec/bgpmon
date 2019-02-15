package modules

import (
	"github.com/CSUNetSec/bgpmon/v2/core"
	"github.com/CSUNetSec/bgpmon/v2/util"
	"net/http"
	_ "net/http/pprof" // Needed so the http ListenAndServe includes the profiler
)

type pprofMod struct {
	*BaseDaemon
}

func (p *pprofMod) Run(addr string, finish core.FinishFunc) error {
	defer finish()

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
	core.RegisterModule("pprof", newpprofModule)
}
