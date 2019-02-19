// Package config defines the constants and functions necessary to parse
// configuration files for bgpmon
package config

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/pkg/errors"
	"io"
	"net"
)

type sessionType int

var (
	//this needs to be in sync with the sessionType enum
	sessionTypeNames = [...]string{
		"cockroachdb",
		"postgres",
	}
)

// These are the session types currently supported
const (
	CochroachSession = sessionType(iota)
	PostgresSession
)

func (s sessionType) String() string {
	return sessionTypeNames[s]
}

// Configer describes the configuration for a bgpmond server
type Configer interface {
	GetSessionConfigs() []SessionConfiger
	GetSessionConfigWithName(string) (SessionConfiger, error)
	GetConfiguredNodes() map[string]NodeConfig
	GetModules() []ModuleConfig
}

// SessionConfiger describes the configuration for a bgpmond session
type SessionConfiger interface {
	Configer
	GetHostNames() []string
	GetName() string
	GetDatabaseName() string
	GetTypeName() string
	GetUser() string
	GetPassword() string
	GetCertDir() string
	GetWorkerCt() int
}

type bgpmondConfig struct {
	DebugOut string
	ErrorOut string
	Sessions map[string]sessionConfig //configured sessions
	Nodes    map[string]NodeConfig    //known nodes. all collectors must be present here
	Modules  map[string]ModuleConfig
}

func (b *bgpmondConfig) GetSessionConfigs() []SessionConfiger {
	ret := make([]SessionConfiger, len(b.Sessions))
	i := 0
	for _, v := range b.Sessions {
		ret[i] = v
		i++
	}
	return ret
}

func (b *bgpmondConfig) GetSessionConfigWithName(a string) (SessionConfiger, error) {
	var (
		ret    sessionConfig
		exists bool
	)
	if ret, exists = b.Sessions[a]; !exists {
		return ret, errors.New(fmt.Sprintf("Session config with name %s does not exist", a))
	}
	return ret, nil
}

func (b *bgpmondConfig) GetConfiguredNodes() map[string]NodeConfig {
	return b.Nodes
}

func (b *bgpmondConfig) GetModules() []ModuleConfig {
	var ret []ModuleConfig
	for _, v := range b.Modules {
		ret = append(ret, v)
	}
	return ret
}

// PutConfiguredNodes writes a node configuration in the TOML format to w
func PutConfiguredNodes(a map[string]NodeConfig, w io.Writer) error {
	return toml.NewEncoder(w).Encode(a)
}

type sessionConfig struct {
	Configer
	name     string   // will be the key of the dictionary, populated after the toml parsing.
	Type     string   // cockroachdb, postgres, etc
	CertDir  string   // directory on the bgpmond host containing the certs
	User     string   // user in the DB to run bgpmond as
	Password string   // user's password
	Hosts    []string // list of hosts for that cluster
	Database string   // the database under which the bgpmond relations live
	WorkerCt int      // The default worker count for this kind of session
}

// NodeConfig describes a BGP node, either a collector or a peer.
type NodeConfig struct {
	IP                  string
	Name                string
	IsCollector         bool
	DumpDurationMinutes int
	Description         string
	Coords              string
	Location            string
}

// ModuleConfig describes a module configuration
type ModuleConfig struct {
	Type string
	ID   string
	Args string
}

func (s sessionConfig) GetName() string {
	return s.name
}

func (s sessionConfig) GetTypeName() string {
	return s.Type
}

func (s sessionConfig) GetHostNames() []string {
	return s.Hosts
}

func (s sessionConfig) GetDatabaseName() string {
	return s.Database
}

func (s sessionConfig) GetUser() string {
	return s.User
}

func (s sessionConfig) GetPassword() string {
	return s.Password
}

func (s sessionConfig) GetCertDir() string {
	return s.CertDir
}

func (s sessionConfig) GetWorkerCt() int {
	return s.WorkerCt
}

// helper function to sanity check the config file.
func (b *bgpmondConfig) checkConfig() error {
	inSlice := false
	var nip net.IP
	for si, s := range b.Sessions {
		for _, stn := range sessionTypeNames {
			if s.Type == stn {
				inSlice = true
			}
		}
		//set the pointer to the parent config to make it satisfy Configer too
		s.Configer = b
		b.Sessions[si] = s
	}
	if !inSlice {
		return fmt.Errorf("unknown session type name. Known session types are: %v", sessionTypeNames)
	}
	for k, v := range b.Nodes {
		if nip = net.ParseIP(k); nip == nil {
			return errors.New(fmt.Sprintf("malformed ip in config:%s", k))
		}
		v.IP = k
		b.Nodes[k] = v
	}

	return nil
}

//NewConfig reads a TOML file with the bgpmon configuration, sanity checks it
//and returns a bgpmondConfig struct which should satisfy the Configer interface,
//or an error
func NewConfig(creader io.Reader) (Configer, error) {
	bc := bgpmondConfig{}
	if _, err := toml.DecodeReader(creader, &bc); err != nil {
		return nil, err
	}
	//the reason that we record the session name in the actual session too,
	//is because we use the toml parsing map function to guarantee unique
	//session names
	for sname, sval := range bc.Sessions {
		sval.name = sname
		bc.Sessions[sname] = sval //update the reference.
	}
	if cerr := bc.checkConfig(); cerr != nil {
		return nil, errors.Wrap(cerr, "config")
	}
	return &bc, nil
}
