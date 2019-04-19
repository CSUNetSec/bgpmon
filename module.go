package bgpmon

import (
	"github.com/CSUNetSec/bgpmon/util"
)

// Types of modules.
const (
	// A task will run until it's finished. It will only be deallocated by
	// the server when it calls its own FinishFunc.
	ModuleTask = iota

	// A daemon will run constantly, and won't stop until it is closed by
	// the server. If it encounters an error, it can call its FinishFunc
	// to be deallocated early.
	ModuleDaemon
)

// Module describes a service that can be started by the server. It can
// interact with the server in any way, including opening sessions,
// streams, and other modules.
type Module interface {
	// Run starts the module, and is guaranteed to launch in a separate
	// goroutine. Args can be used to pass any information the module
	// needs.
	Run(args map[string]string)

	// GetType should return one of the module types defined above.
	GetType() int

	// GetName returns a string to identify the module type. Each
	// instance of a module should share the same name.
	GetName() string

	// GetInfo returns a struct containing info that describes the
	// running module.
	GetInfo() OpenModuleInfo

	// Stop is called to prematurely cancel a running module. The
	// module should be deallocated just after calling this. Stop
	// should be blocking until it knows the module has been stopped.
	Stop() error
}

// ModuleInfo is used to describe an available module. It should include the
// type of the module, a description, and a description of the opts that the
// module uses.
type ModuleInfo struct {
	Type        string
	Description string
	Opts        string
}

// OpenModuleInfo describes an actively running module. It should include the
// type of the module, the ID, and a string describing the status of the
// module. Modules don't keep track of their own ID, so that field must be
// populated by the server.
type OpenModuleInfo struct {
	Type   string
	ID     string
	Status string
}

// NewOpenModuleInfo returns an info struct with a type and a status, and
// leaves the ID to be populated by something else.
func NewOpenModuleInfo(modType string, status string) OpenModuleInfo {
	return OpenModuleInfo{Type: modType, ID: "", Status: status}
}

// ModuleHandler wraps the info and maker types. It's used to store modules
// globally, and as the argument to register new modules.
type ModuleHandler struct {
	Info  ModuleInfo
	Maker ModuleMaker
}

// ModuleMaker is a function to instantiate a module. It is given a handle
// to the BgpmondServer, and a logger to print information.
type ModuleMaker func(BgpmondServer, util.Logger) Module

// This map holds all of the registered ModuleHandlers
var knownModules map[string]ModuleHandler

func init() {
	knownModules = make(map[string]ModuleHandler)
}

// RegisterModule adds a module type to a list of known modules. Once a
// module maker is registered, the server can create and launch modules
// of this type
func RegisterModule(handle ModuleHandler) {
	typeName := handle.Info.Type
	_, exists := knownModules[typeName]
	if exists {
		coreLogger.Infof("Module type: %s already exists, ignoring new registration.")
		return
	}

	knownModules[typeName] = handle
}

// getModuleMaker returns the creator function associated with this type
// name. If this type name isn't registered, it returns nil, and false.
func getModuleMaker(typeName string) (ModuleMaker, bool) {
	handle, exists := knownModules[typeName]
	if !exists {
		return nil, false
	}
	return handle.Maker, true
}

// getModuleLogger returns a util.Logger with fields specific to this module.
// This ensures all modules log in the same format.
func getModuleLogger(modType, modName string) util.Logger {
	return util.NewLogger("system", "module", "type", modType, "ID", modName)
}

// getModuleTypes returns the info on all modules registered to the
// server.
func getModuleTypes() []ModuleInfo {
	var ret []ModuleInfo
	for _, v := range knownModules {
		ret = append(ret, v.Info)
	}
	return ret
}
