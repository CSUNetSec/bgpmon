package session

import (
	"fmt"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon"
)

type FileConfig struct {
}

type FileSession struct {
	*Session
}

func NewFileSession(filename string, config FileConfig) (Sessioner, error) {
	writers := make(map[pb.WriteRequest_Type][]Writer)

	session, err := NewSession(writers, 1)
	if err != nil {
		return nil, err
	}

	return FileSession{&session}, nil
}

func (f FileSession) Close() error {
	return nil
}

func (f FileSession) Write(w *pb.WriteRequest) error {
	fmt.Printf("got request:%+v\n", w)
	return nil
}
