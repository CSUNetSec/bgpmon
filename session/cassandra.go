package session

import (
	"errors"

	"github.com/gocql/gocql"
)

type CassandraSession struct {
	session *gocql.Session
}

func NewCassandraSession(username, password string, hosts []string) (Session, error) {
	return nil, errors.New("unimplemented")
}
