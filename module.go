package bgpmon

import (
	"fmt"

	"github.com/CSUNetSec/bgpmon/util"
)

// Types of modules.
const (
	// A task will run until it's finished. It will only be deallocated by
	// the server when it calls it's own FinishFunc.
	ModuleTask = iota

	// A daemon will run constantly, and won't stop until it is closed by
	// the server. If it encounters an error, it can call it's FinishFunc
	// to be deallocated early.
	ModuleDaemon
)

// Module describes a process that can be started by the server. It can
// interact with the server in any way, including opening sessions,
// streams, and other modules.
type Module interface {
	// Run starts the process, and is guaranteed to launch in a separate
	// goroutine. Args can be used to pass any information the module
	// needs. A module can call its FinishFunc at any time to be deallocated
	// by the server
	Run(args map[string]string, f FinishFunc) error

	// GetType should return one of ModuleTask, or ModuleDaemon
	GetType() int

	// GetName returns a string to identify the module type. Each
	// instance of a module should share the same name
	GetName() string

	// Stop is called to prematurely cancel a running module. The
	// module should be deallocated just after calling this.
	Stop() error
}

// FinishFunc is a function passed from the server to a new module. It can be
// called by the new module to let the server know the module can be deallocated.
type FinishFunc func()

// ModuleMaker is a function to instanciate a module. It is given a handle
// to the BgpmondServer, and a logger to print information.
type ModuleMaker func(BgpmondServer, util.Logger) Module

var knownModules map[string]ModuleMaker

func init() {
	knownModules = make(map[string]ModuleMaker)
}

// RegisterModule adds a module type to a list of known modules. Once a
// module maker is registered, the server can create and launch modules
// of this type
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
	return util.NewLogger("system", "module", "type", modType, "ID", modName)
}

func getModuleTypes() []string {
	var ret []string
	for k := range knownModules {
		ret = append(ret, k)
	}
	return ret
}
