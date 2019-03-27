// Package modules defines commonly used modules for bgpmon. It also defines some example
// modules that developers could build off of.
package modules

import (
	core "github.com/CSUNetSec/bgpmon"
	"github.com/CSUNetSec/bgpmon/util"
)

// BaseTask is an example task module which can usefully composed in other modules.
type BaseTask struct {
	core.Module

	server core.BgpmondServer
	logger util.Logger
	name   string
}

// Run satisfies the module interface
func (b *BaseTask) Run(launchOpts map[string]string, finish core.FinishFunc) error {
	defer finish()

	b.logger.Infof("Example task run with opts:%v", launchOpts)
	return nil
}

// GetType satisfies the module interface
func (b *BaseTask) GetType() int {
	return core.ModuleTask
}

// GetName satisfies the module interface
func (b *BaseTask) GetName() string {
	return b.name
}

// Stop satisfies the module interface
func (b *BaseTask) Stop() error {
	return nil
}

// NewBaseTask is the ModuleMaker function for a BaseTask
func NewBaseTask(server core.BgpmondServer, logger util.Logger, name string) *BaseTask {
	return &BaseTask{server: server, logger: logger, name: name}
}

// BaseDaemon is an example daemon module which can be usefully composed in other modules.
type BaseDaemon struct {
	*BaseTask
	cancel chan bool
}

// Run satisfies the module interface
func (b *BaseDaemon) Run(launchOpts map[string]string, _ core.FinishFunc) error {
	b.logger.Infof("Example daemon run with: %v", launchOpts)
	<-b.cancel
	b.logger.Infof("Example daemon closed")
	return nil
}

// GetType satisfies the module interface
func (b *BaseDaemon) GetType() int {
	return core.ModuleDaemon
}

// Stop satisfies the module interface
func (b *BaseDaemon) Stop() error {
	close(b.cancel)
	return nil
}

// NewBaseDaemon is the ModuleMaker function for a BaseDaemon
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
