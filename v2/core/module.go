package core

import (
	"fmt"
	"github.com/CSUNetSec/bgpmon/v2/util"
)

// Types of modules
const (
	MODULE_TASK = iota
	MODULE_DAEMON
)

// Stop Codes
const (
	MODULE_STOP_CANCEL = iota
	MODULE_STOP_FINISHED
)

type Module interface {
	Run(string, FinishFunc) error
	GetType() int
	GetName() string
	Stop() error
}

// This is called by a module when it is finished to let the server know
// it can deallocate it
type FinishFunc func()

type ModuleMaker func(BgpmondServer, util.Logger) Module

var knownModules map[string]ModuleMaker

func init() {
	knownModules = make(map[string]ModuleMaker)
}

func RegisterModule(typeName string, makeNew ModuleMaker) error {
	_, exists := knownModules[typeName]
	if exists {
		return fmt.Errorf("Module type: %s already exists", typeName)
	}

	knownModules[typeName] = makeNew
	return nil
}

func GetModuleMaker(typeName string) (ModuleMaker, bool) {
	maker, exists := knownModules[typeName]
	return maker, exists
}

func GetModuleLogger(modType, modName string) util.Logger {
	return util.NewLogger("system", "module", "module type", modType, "ID", modName)
}

func GetModuleTypes() []string {
	var ret []string
	for k, _ := range knownModules {
		ret = append(ret, k)
	}
	return ret
}
