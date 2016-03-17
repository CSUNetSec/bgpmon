package session

import (
	"errors"
	"fmt"

	pb "github.com/CSUNetSec/bgpmon/protobuf"

	"github.com/gocql/gocql"
)

type CassandraConfig struct {
	MessageTypes map[string][]InsertConfig
}

type InsertConfig struct {
	Statement string
	Values    []string
}

type CassandraSession struct {
	session          *gocql.Session
	insertStatements map[pb.WriteRequest_Type][]InsertConfig
}

func NewCassandraSession(username, password string, hosts []string, config CassandraConfig) (Session, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.LocalOne
	cluster.ProtoVersion = 4
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{10}
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password}
	cluster.NumConns = 16

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	insertStatements := make(map[pb.WriteRequest_Type][]InsertConfig)
	for key, value := range config.MessageTypes {
		switch key {
		case "ASNumberLocation":
			insertStatements[pb.WriteRequest_AS_NUMBER_LOCATION] = value
		case "BGPUpdateMessage":
			insertStatements[pb.WriteRequest_BGP_UPDATE] = value
		case "IPAddressLocation":
			insertStatements[pb.WriteRequest_IP_ADDRESS_LOCATION] = value
		case "PrefixLocation":
			insertStatements[pb.WriteRequest_PREFIX_LOCATION] = value
		default:
			return nil, errors.New(fmt.Sprintf("Unknown message type '%s' in cassandra session", key))
		}
	}

	cassSession := CassandraSession{session, insertStatements}
	return cassSession, nil
}

func (c CassandraSession) Close() error {
	c.session.Close()
	return nil
}

func (c CassandraSession) Write(string) error {
	return errors.New("unimplemented")
}
