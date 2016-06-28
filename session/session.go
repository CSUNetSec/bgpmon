package session

import (
	"errors"
    "fmt"

	pb "github.com/CSUNetSec/bgpmon/protobuf"
)

type Session struct {
    workerChans []chan *pb.WriteRequest
    workerIndex int
}

func NewSession(writers map[pb.WriteRequest_Type][]Writer, workerCount int) (Session, error) {
    workerChans := make([]chan *pb.WriteRequest, workerCount)
    for i := 0; i<workerCount; i++ {
        workerChan := make(chan *pb.WriteRequest)
        go func() {
            for {
                writeRequest := <-workerChan
                writers, exists := writers[writeRequest.Type]
                if !exists {
                    //TODO get an error message back somehow
                    panic(errors.New(fmt.Sprintf("Unable to write type '%v' because it doesn't exist", writeRequest.Type)))
                }

                for _, writer := range writers {
                    if err := writer.Write(writeRequest); err != nil {
                        //TODO return error
                        panic(err)
                    }
                }
            }
        }()

        workerChans[i] = workerChan
    }

    return Session{workerChans, 0}, nil
}

func (s *Session) Write(w *pb.WriteRequest) error {
    s.workerChans[s.workerIndex] <- w
    s.workerIndex = (s.workerIndex + 1) % len(s.workerChans)

	return nil
}

type Sessioner interface {
	Close() error
	Write(*pb.WriteRequest) error
}

type Writer interface {
	Write(*pb.WriteRequest) error
}
