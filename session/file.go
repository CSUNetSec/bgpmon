package session

import (
	"errors"

	pb "github.com/CSUNetSec/bgpmon/protobuf"
)

type FileConfig struct {
}

type FileSession struct {
	Session
}

func NewFileSession(filename string, config FileConfig) (Sessioner, error) {
	writers := make(map[pb.WriteRequest_Type][]Writer)

	return FileSession{Session{writers}}, nil
}

func (f FileSession) Close() error {
	return nil
}

func (f FileSession) Write(w *pb.WriteRequest) error {
	return errors.New("unimplemented")
}
