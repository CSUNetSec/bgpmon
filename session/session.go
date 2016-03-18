package session

import (
	"errors"

	pb "github.com/CSUNetSec/bgpmon/protobuf"
)

type Session struct {
	Writers map[pb.WriteRequest_Type][]Writer
}

func (s Session) Write(w *pb.WriteRequest) error {
	writers, exists := s.Writers[w.Type]
	if !exists {
		return errors.New("WriteRequest type not configured")
	}

	for _, writer := range writers {
		writer.Write(w)
	}

	return nil
}

type Sessioner interface {
	Close() error
	Write(*pb.WriteRequest) error
}

type Writer interface {
	Write(*pb.WriteRequest) error
}
