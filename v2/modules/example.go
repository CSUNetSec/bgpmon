package modules

import (
	"github.com/CSUNetSec/bgpmon/v2/core"
	"github.com/CSUNetSec/bgpmon/v2/util"
)

type BaseTask struct {
	server core.BgpmondServer
	logger util.Logger
	name   string
}

func (b *BaseTask) Run(launchStr string, finish core.FinishFunc) error {
	defer finish()

	b.logger.Infof("Example task run with: %s", launchStr)
	return nil
}

func (b *BaseTask) GetType() int {
	return core.MODULE_TASK
}

func (b *BaseTask) GetName() string {
	return b.name
}

func (b *BaseTask) Stop() error {
	return nil
}

func NewBaseTask(server core.BgpmondServer, logger util.Logger, name string) *BaseTask {
	return &BaseTask{server: server, logger: logger, name: name}
}

type BaseDaemon struct {
	*BaseTask
	cancel chan bool
}

func (b *BaseDaemon) Run(launchStr string, _ core.FinishFunc) error {
	b.logger.Infof("Example daemon run with: %s", launchStr)
	<-b.cancel
	b.logger.Infof("Example daemon closed")
	return nil
}

func (b *BaseDaemon) GetType() int {
	return core.MODULE_DAEMON
}

func (b *BaseDaemon) Stop() error {
	close(b.cancel)
	return nil
}

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
