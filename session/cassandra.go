package session

import (
	"errors"

	"github.com/gocql/gocql"
)

type CassandraSession struct {
	session *gocql.Session
}

func NewCassandraSession(username, password string, hosts []string) (Session, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.LocalOne
	cluster.ProtoVersion = 4
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy { 10 }
	cluster.Authenticator = gocql.PasswordAuthenticator { Username: username, Password: password }
	cluster.NumConns = 16

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	cassSession := CassandraSession { session }
	return cassSession, nil
}

func (c CassandraSession) Close() error {
	c.session.Close()
	return nil
}

func (c CassandraSession) Write(string) error {
	return errors.New("unimplemented")
}
