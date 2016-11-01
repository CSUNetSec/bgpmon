package session

import (
	"github.com/CSUNetSec/bgpmon/log"

	pb "github.com/CSUNetSec/bgpmon/pb"
)

type Session struct {
	workerChans []chan *pb.WriteRequest
	workerIndex int
}

func NewSession(writers map[pb.WriteRequest_Type][]Writer, workerCount uint32) (Session, error) {
	workerChans := make([]chan *pb.WriteRequest, workerCount)

	for i := 0; i < int(workerCount); i++ {
		workerChan := make(chan *pb.WriteRequest)
		go func(wc chan *pb.WriteRequest) {
			for {
				select {
				case writeRequest := <-wc:
					writers, exists := writers[writeRequest.Type]
					if !exists {
						//TODO get an error message back somehow
						//panic(errors.New(fmt.Sprintf("Unable to write type '%v' because it doesn't exist", writeRequest.Type)))
						log.Errl.Printf("Unable to write type '%v' because it doesn't exist", writeRequest.Type)
					}

					for _, writer := range writers {
						//fmt.Printf("writing in writer :%v\n", writer)
						if err := writer.Write(writeRequest); err != nil {
							log.Errl.Printf("error from worker for write request:%+v on writer:%+v error:%s\n", writeRequest, writer, err)
							break
						}
					}
				}
			}
		}(workerChan)

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
