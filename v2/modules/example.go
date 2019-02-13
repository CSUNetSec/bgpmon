package modules

import (
	"github.com/CSUNetSec/bgpmon/v2/core"
	"github.com/CSUNetSec/bgpmon/v2/util"
)

// BaseTask ... An example task module which can usefully composed in other modules
type BaseTask struct {
	server core.BgpmondServer
	logger util.Logger
	name   string
}

// Run ... Satisfies the module interface
func (b *BaseTask) Run(launchStr string, finish core.FinishFunc) error {
	defer finish()

	b.logger.Infof("Example task run with: %s", launchStr)
	return nil
}

// GetType ... Satisfies the module interface
func (b *BaseTask) GetType() int {
	return core.ModuleTask
}

// GetName ... Satisfies the module interface
func (b *BaseTask) GetName() string {
	return b.name
}

// Stop ... Satisfies the module interface
func (b *BaseTask) Stop() error {
	return nil
}

// NewBaseTask ... The ModuleMaker function for a BaseTask
func NewBaseTask(server core.BgpmondServer, logger util.Logger, name string) *BaseTask {
	return &BaseTask{server: server, logger: logger, name: name}
}

// BaseDaemon ... An example daemon module which can be usefully composed in other modules
type BaseDaemon struct {
	*BaseTask
	cancel chan bool
}

// Run ... Satisfies the module interface
func (b *BaseDaemon) Run(launchStr string, _ core.FinishFunc) error {
	b.logger.Infof("Example daemon run with: %s", launchStr)
	<-b.cancel
	b.logger.Infof("Example daemon closed")
	return nil
}

// GetType ... Satisfies the module interface
func (b *BaseDaemon) GetType() int {
	return core.ModuleDaemon
}

// Stop ... Satisfies the module interface
func (b *BaseDaemon) Stop() error {
	close(b.cancel)
	return nil
}

// NewBaseDaemon ... The ModuleMaker function for a BaseDaemon
func NewBaseDaemon(server core.BgpmondServer, logger util.Logger, name string) *BaseDaemon {
	cancel := make(chan bool)
	return &BaseDaemon{BaseTask: NewBaseTask(server, logger, name), cancel: cancel}
}

func init() {
	core.RegisterModule("example_task", func(s core.BgpmondServer, l util.Logger) core.Module {
		return NewBaseTask(s, l, "example_task")
	})

	core.RegisterModule("example_daemon", func(s core.BgpmondServer, l util.Logger) core.Module {
		return NewBaseDaemon(s, l, "example_daemon")
	})
}
