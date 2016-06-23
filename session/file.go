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

    session, err := NewSession(writers, 1)
    if err != nil {
        return nil, err
    }

	return FileSession{session}, nil
}

func (f FileSession) Close() error {
	return nil
}

func (f FileSession) Write(w *pb.WriteRequest) error {
	return errors.New("unimplemented")
}
