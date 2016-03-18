package session

import (
	//"errors"
	//"fmt"

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
	Session
	cqlSession *gocql.Session
	//insertStatements map[pb.WriteRequest_Type][]InsertConfig
}

func NewCassandraSession(username, password string, hosts []string, config CassandraConfig) (Sessioner, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.LocalOne
	cluster.ProtoVersion = 4
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{10}
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password}
	cluster.NumConns = 16

	cqlSession, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	writers := make(map[pb.WriteRequest_Type][]Writer)
	/*insertStatements := make(map[pb.WriteRequest_Type][]InsertConfig)
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
	}*/

	cassSession := CassandraSession{Session{writers}, cqlSession}
	return cassSession, nil
}

func (c CassandraSession) Close() error {
	c.cqlSession.Close()
	return nil
}

/*
 * Writers
 */
type CassandraWriter struct {
	cqlSession *gocql.Session
}

type BGPUpdateWriter struct {
	CassandraWriter
	keyspaces  []string
}

func (b BGPUpdateWriter) Write(request *pb.WriteRequest) error {
	return nil
}

type LocationByASWriter struct {
	CassandraWriter
	keyspaces []string
}

func (l LocationByASWriter) Write(request *pb.WriteRequest) error {
	return nil
}
