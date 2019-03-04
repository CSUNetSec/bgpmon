// Package bgpmon provides the core interfaces for developing bgpmon client programs
// and modules.
package bgpmon

import (
	"fmt"
	"github.com/CSUNetSec/bgpmon/config"
	"github.com/CSUNetSec/bgpmon/db"
	"github.com/CSUNetSec/bgpmon/util"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"os"
	"sync"
)

var (
	corelogger = util.NewLogger("system", "core")
)

// BgpmondServer is the interface for interacting with a server instance.
// It provides function to open and close sessions and modules, and get
// data on the state on the server.
type BgpmondServer interface {
	// Opens a session of type sType, which must come from the config file.
	// The ID of this session is sID, which is used to interact with this
	// session, and wc is the worker count, or 0, to use a default wc of 1.
	OpenSession(sType, sID string, wc int) error
	ListSessionTypes() []*pb.SessionType
	ListSessions() []SessionHandle
	CloseSession(string) error
	OpenWriteStream(string) (db.WriteStream, error)
	OpenReadStream(string) (db.ReadStream, error)

	RunModule(string, string, map[string]string) error
	ListModuleTypes() []string
	ListRunningModules() []string
	CloseModule(string) error
	Close() error
}

// SessionHandle is used to return information on an open session.
type SessionHandle struct {
	Name     string
	SessType *pb.SessionType
	Session  *db.Session
}

type server struct {
	sessions map[string]SessionHandle
	modules  map[string]Module
	conf     config.Configer
	mux      *sync.Mutex
}

// NewServer creates a BgpmondServer instance from a configuration. It loads
// session types from the configuration, and launches any modules specified.
// It returns an error if a module type is specified that isn't registered
// with the server.
func NewServer(conf config.Configer) (BgpmondServer, error) {
	s := &server{}
	s.sessions = make(map[string]SessionHandle)
	s.modules = make(map[string]Module)
	s.mux = &sync.Mutex{}
	s.conf = conf

	for _, mod := range conf.GetModules() {
		err := s.RunModule(mod.GetType(), mod.GetID(), mod.GetArgs())
		if err != nil {
			s.Close()
			return nil, err
		}
	}
	return s, nil
}

// NewServerFromFile does the same thing as NewServer, but loads the
// configuration from a specified file name. Returns an error if the
// file can't be parsed, or specifies an invalid module.
func NewServerFromFile(fName string) (BgpmondServer, error) {
	fd, err := os.Open(fName)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	bc, err := config.NewConfig(fd)
	if err != nil {
		return nil, err
	}

	return NewServer(bc)
}

func (s *server) OpenSession(sType, sID string, workers int) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	if _, ok := s.sessions[sID]; ok {
		return corelogger.Errorf("Session ID: %s already exists.", sID)
	}

	sc, err := s.conf.GetSessionConfigWithName(sType)
	if err != nil {
		return corelogger.Errorf("No session type with session type name: %s found", sType)
	}

	session, err := db.NewSession(sc, sID, workers)
	if err != nil {
		return corelogger.Errorf("Create session failed: %v", err)
	}

	hosts := fmt.Sprintf("Hosts: %v", sc.GetHostNames())
	stype := &pb.SessionType{Name: sc.GetName(), Type: sc.GetTypeName(), Desc: hosts}
	sh := SessionHandle{Name: sID, SessType: stype, Session: session}
	s.sessions[sID] = sh

	return nil
}

func (s *server) CloseSession(sID string) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	sh, ok := s.sessions[sID]
	if !ok {
		return corelogger.Errorf("No session found with ID: %s", sID)
	}

	sh.Session.Close()
	delete(s.sessions, sID)

	return nil
}

func (s *server) CloseAllSessions() {
	s.mux.Lock()
	defer s.mux.Unlock()

	for k, v := range s.sessions {
		v.Session.Close()
		delete(s.sessions, k)
	}

	return
}

func (s *server) ListSessionTypes() []*pb.SessionType {
	availSessions := []*pb.SessionType{}
	for _, sc := range s.conf.GetSessionConfigs() {
		availsess := &pb.SessionType{
			Name: sc.GetName(),
			Type: sc.GetTypeName(),
			Desc: fmt.Sprintf("Hosts: %v", sc.GetHostNames()),
		}

		availSessions = append(availSessions, availsess)
	}
	return availSessions
}

func (s *server) ListSessions() []SessionHandle {
	s.mux.Lock()
	defer s.mux.Unlock()

	sList := []SessionHandle{}
	for _, sh := range s.sessions {
		sList = append(sList, sh)
	}
	return sList
}

func (s *server) OpenWriteStream(sID string) (db.WriteStream, error) {
	s.mux.Lock()
	sh, ok := s.sessions[sID]
	if !ok {
		return nil, corelogger.Errorf("Can't open stream on nonexistant session: %s", sID)
	}
	s.mux.Unlock()

	stream, err := sh.Session.OpenWriteStream(db.SessionWriteCapture)
	if err != nil {
		return nil, corelogger.Errorf("Failed to open stream on session(%s): %s", sID, err)
	}

	return stream, nil
}

func (s *server) OpenReadStream(sID string) (db.ReadStream, error) {
	return nil, nil

}

func (s *server) RunModule(modType, name string, modargs map[string]string) error {
	corelogger.Infof("Running module %s with ID %s", modType, name)
	s.mux.Lock()
	defer s.mux.Unlock()

	if _, ok := s.modules[name]; ok {
		return corelogger.Errorf("Module with ID: %s is already running", name)
	}

	maker, ok := getModuleMaker(modType)
	if !ok {
		return corelogger.Errorf("No module type: %s found", modType)
	}

	newMod := maker(s, getModuleLogger(modType, name))
	s.modules[name] = newMod
	go newMod.Run(modargs, s.getFinishFunc(name))
	return nil
}

func (s *server) getFinishFunc(id string) FinishFunc {
	return func() {
		s.mux.Lock()
		delete(s.modules, id)
		s.mux.Unlock()
	}
}

func (s *server) ListModuleTypes() []string {
	return getModuleTypes()
}

func (s *server) ListRunningModules() []string {
	s.mux.Lock()
	defer s.mux.Unlock()

	var ret []string
	for k, v := range s.modules {
		ret = append(ret, fmt.Sprintf("ID: %s NAME: %s TYPE: %d", k, v.GetName(), v.GetType()))
	}
	return ret
}

func (s *server) CloseModule(name string) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	mod, ok := s.modules[name]
	if !ok {
		return corelogger.Errorf("No module with ID: %s found", name)
	}

	mod.Stop()
	delete(s.modules, name)
	return nil
}

func (s *server) CloseAllModules() {
	s.mux.Lock()
	defer s.mux.Unlock()

	for k, v := range s.modules {
		v.Stop()
		delete(s.modules, k)
	}
}

func (s *server) Close() error {
	s.CloseAllModules()
	s.CloseAllSessions()
	return nil
}
