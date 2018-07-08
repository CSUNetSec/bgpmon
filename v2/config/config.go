package config

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/pkg/errors"
	"io"
)

type sessionType int

var (
	//this needs to be in sync with the sessionType enum
	sessionTypeNames = [...]string{
		"cockroachdb",
		"postgres",
	}
)

const (
	CochroachSession = sessionType(iota)
	PostgresSession
)

func (s sessionType) String() string {
	return sessionTypeNames[s]
}

type Configer interface {
	GetSessionConfigs() []SessionConfiger
	GetSessionConfigWithName(string) (SessionConfiger, error)
	GetListenAddress() string
}

type SessionConfiger interface {
	GetHostNames() []string
	GetName() string
	GetDatabaseName() string
	GetTypeName() string
	GetUser() string
	GetCertDir() string
}

type bgpmondConfig struct {
	Address  string
	DebugOut string
	ErrorOut string
	Sessions map[string]sessionConfig
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

func (b *bgpmondConfig) GetListenAddress() string {
	return b.Address
}

type sessionConfig struct {
	name     string   // will be the key of the dictionary, populated after the toml parsing.
	Type     string   // cockroachdb, postgres, etc
	CertDir  string   // directory on the bgpmond host containing the certs
	User     string   // user in the DB to run bgpmond as
	Hosts    []string // list of hosts for that cluster
	Database string   // the database under which the bgpmond relations live
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

func (s sessionConfig) GetCertDir() string {
	return s.CertDir
}

// helper function to sanity check the config file.
func checkConfig(c bgpmondConfig) error {
	inSlice := false
	for _, s := range c.Sessions {
		for _, stn := range sessionTypeNames {
			if s.Type == stn {
				inSlice = true
			}
		}
	}
	if !inSlice {
		return errors.New(fmt.Sprintf("unknown session type name. Known session types are:%v\n", sessionTypeNames))
	}
	return nil
}

//NewConfig reads a TOML file with the bgpmon configuration, sanity checks it
//and returns a bgpmondConfig struct which should satisfy the Configer interface,
//or an error
func NewConfig(creader io.Reader) (*bgpmondConfig, error) {
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
	if cerr := checkConfig(bc); cerr != nil {
		return nil, errors.Wrap(cerr, "config")
	}
	return &bc, nil
}
