// Package modules defines commonly used modules for bgpmon. It also defines some example
// modules that developers could build off of.
package modules

import (
	"context"
	"fmt"
	"sync"

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
func (b *BaseTask) Run(launchOpts map[string]string, finish core.FinishFunc) {
	defer finish()

	b.logger.Infof("Example task run with opts:%v", launchOpts)
	return
}

// GetType satisfies the module interface
func (b *BaseTask) GetType() int {
	return core.ModuleTask
}

// GetName satisfies the module interface
func (b *BaseTask) GetName() string {
	return b.name
}

// GetInfo satisfies the module interface
func (b *BaseTask) GetInfo() core.OpenModuleInfo {
	return core.NewOpenModuleInfo(b.name, "Running")
}

// Stop satisfies the module interface. A task is not stoppable.
func (b *BaseTask) Stop() error {
	return fmt.Errorf("tasks can't be stopped")
}

// NewBaseTask is the ModuleMaker function for a BaseTask
func NewBaseTask(server core.BgpmondServer, logger util.Logger, name string) *BaseTask {
	return &BaseTask{server: server, logger: logger, name: name}
}

// BaseDaemon is an example daemon module which can be usefully composed in other modules.
type BaseDaemon struct {
	*BaseTask

	wg  *sync.WaitGroup
	ctx context.Context
	cf  context.CancelFunc
}

// Run satisfies the module interface, and prints waits for the module to
// be closed.
func (b *BaseDaemon) Run(launchOpts map[string]string, _ core.FinishFunc) {
	defer b.wg.Done()

	b.logger.Infof("Example daemon run with: %v", launchOpts)
	<-b.ctx.Done()
	b.logger.Infof("Example daemon closed")
	return
}

// GetType satisfies the module interface
func (b *BaseDaemon) GetType() int {
	return core.ModuleDaemon
}

// Stop satisfies the module interface. For a daemon module, this means
// cancelling the context and waiting for the Run function to complete.
func (b *BaseDaemon) Stop() error {
	b.cf()
	b.wg.Wait()
	return nil
}

// NewBaseDaemon is the ModuleMaker function for a BaseDaemon
func NewBaseDaemon(server core.BgpmondServer, logger util.Logger, name string) *BaseDaemon {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)
	return &BaseDaemon{BaseTask: NewBaseTask(server, logger, name), wg: wg, ctx: ctx, cf: cancel}
}

func init() {
	taskHandle := core.ModuleHandler{
		Info: core.ModuleInfo{
			Type:        "example_task",
			Description: "Run an example task module!",
			Opts:        "None",
		},
		Maker: func(s core.BgpmondServer, l util.Logger) core.Module {
			return NewBaseTask(s, l, "example_task")
		},
	}
	core.RegisterModule(taskHandle)

	daemonHandle := core.ModuleHandler{
		Info: core.ModuleInfo{
			Type:        "example_daemon",
			Description: "Run an example daemon module!",
			Opts:        "None",
		},
		Maker: func(s core.BgpmondServer, l util.Logger) core.Module {
			return NewBaseDaemon(s, l, "example_daemon")
		},
	}
	core.RegisterModule(daemonHandle)
}
