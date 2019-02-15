package core

import (
	"fmt"
	"github.com/CSUNetSec/bgpmon/util"
)

// Types of modules
const (
	ModuleTask = iota
	ModuleDaemon
)

// Module ... A common interface all modules must satisfy
type Module interface {
	Run(string, FinishFunc) error
	GetType() int
	GetName() string
	Stop() error
}

// FinishFunc ... This is called by a module when it is finished to let the server know
// it can deallocate it
type FinishFunc func()

// ModuleMaker ...  Describes any function that creates a new module
type ModuleMaker func(BgpmondServer, util.Logger) Module

var knownModules map[string]ModuleMaker

func init() {
	knownModules = make(map[string]ModuleMaker)
}

// RegisterModule ... Register a module creator with the server
func RegisterModule(typeName string, makeNew ModuleMaker) error {
	_, exists := knownModules[typeName]
	if exists {
		return fmt.Errorf("Module type: %s already exists", typeName)
	}

	knownModules[typeName] = makeNew
	return nil
}

func getModuleMaker(typeName string) (ModuleMaker, bool) {
	maker, exists := knownModules[typeName]
	return maker, exists
}

func getModuleLogger(modType, modName string) util.Logger {
	return util.NewLogger("system", "module", "module type", modType, "ID", modName)
}

func getModuleTypes() []string {
	var ret []string
	for k := range knownModules {
		ret = append(ret, k)
	}
	return ret
}
