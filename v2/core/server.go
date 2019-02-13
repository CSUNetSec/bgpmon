package core

import (
	"context" // XXX: Get rid of this when session no longer needs it
	"fmt"
	"github.com/CSUNetSec/bgpmon/v2/config"
	"github.com/CSUNetSec/bgpmon/v2/db"
	"github.com/CSUNetSec/bgpmon/v2/util"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"os"
	"sync"
)

var (
	corelogger = util.NewLogger("system", "core")
)

// BgpmondServer ...
type BgpmondServer interface {
	OpenSession(string, string, int) error
	ListSessionTypes() []*pb.SessionType
	ListSessions() []SessionHandle
	CloseSession(string) error
	OpenWriteStream(string) (*db.SessionStream, error)

	RunModule(string, string, string) error
	ListModuleTypes() []string
	ListRunningModules() []string
	CloseModule(string) error
	Close() error
}

// SessionHandle ... Structure to wrap the session and session type information
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

// NewServer ... Create a new server from a configuration
func NewServer(conf config.Configer) BgpmondServer {
	s := &server{}
	s.sessions = make(map[string]SessionHandle)
	s.modules = make(map[string]Module)
	s.mux = &sync.Mutex{}
	s.conf = conf
	return s
}

// NewServerFromFile ... Create a new server, loading the configuration from a file
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

	return NewServer(bc), nil
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

	session, err := db.NewSession(context.Background(), sc, sID, workers)
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

// Does this need to lock the mutex because it reads s.sessions?
func (s *server) OpenWriteStream(sID string) (*db.SessionStream, error) {
	sh, ok := s.sessions[sID]
	if !ok {
		return nil, corelogger.Errorf("Can't open stream on nonexistant session: %s", sID)
	}

	stream, err := sh.Session.Do(db.SessionOpenStream, nil)
	if err != nil {
		return nil, corelogger.Errorf("Failed to open stream on session(%s): %s", sID, err)
	}

	return stream, nil
}

func (s *server) RunModule(modType, name, launchStr string) error {
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
	go newMod.Run(launchStr, s.GetFinishFunc(name))
	return nil
}

func (s *server) GetFinishFunc(id string) FinishFunc {
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
