package session

import (
	"errors"
    "fmt"

	pb "github.com/CSUNetSec/bgpmon/protobuf"
)

type Session struct {
    workerChans []chan *pb.WriteRequest
    errChans []chan error
    workerIndex int
}

func NewSession(writers map[pb.WriteRequest_Type][]Writer, workerCount uint32) (Session, error) {
    workerChans := make([]chan *pb.WriteRequest, workerCount)
    errChans := make([]chan error, workerCount)

    for i := 0; i<int(workerCount); i++ {
        workerChan := make(chan *pb.WriteRequest)
        errChan := make(chan error)
        go func() {
            for {
                writeRequest := <-workerChan
                writers, exists := writers[writeRequest.Type]
                if !exists {
                    //TODO get an error message back somehow
                    //panic(errors.New(fmt.Sprintf("Unable to write type '%v' because it doesn't exist", writeRequest.Type)))
                    errChan <- errors.New(fmt.Sprintf("Unable to write type '%v' because it doesn't exist", writeRequest.Type))
                }

                for _, writer := range writers {
                    err := writer.Write(writeRequest)

                    errChan <- err
                    /*if err := writer.Write(writeRequest); err != nil {
                        //TODO return error
                        panic(err)
                    }*/
                }
            }
        }()

        workerChans[i] = workerChan
        errChans[i] = errChan
    }

    return Session{workerChans, errChans, 0}, nil
}

func (s *Session) Write(w *pb.WriteRequest) error {
    s.workerChans[s.workerIndex] <- w
    err := <-s.errChans[s.workerIndex]

    s.workerIndex = (s.workerIndex + 1) % len(s.workerChans)
    return err
}

type Sessioner interface {
    Close() error
    Write(*pb.WriteRequest) error
}

type Writer interface {
    Write(*pb.WriteRequest) error
}
