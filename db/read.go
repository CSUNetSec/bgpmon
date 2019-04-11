package db

import (
	"context"
	"io"
)

type readCapStream struct {
	*sessionStream

	lastRep *CommonReply
	lastErr error

	dbResp chan CommonReply
	cancel chan bool
}

func (rcs *readCapStream) Read() bool {
	msg, ok := <-rcs.dbResp

	if !ok {
		rcs.lastErr = io.EOF
		return false
	}

	if msg.Error() != nil {
		rcs.lastErr = msg.Error()
		return false
	}

	rcs.lastRep = &msg
	return true
}

func (rcs *readCapStream) Data() interface{} {
	if rcs.lastRep == nil {
		return nil
	}

	capMsg := (*rcs.lastRep).(getCapReply)
	return capMsg.getCapture()
}

func (rcs *readCapStream) Bytes() []byte {
	if rcs.lastRep == nil {
		return nil
	}

	capMsg := (*rcs.lastRep).(getCapReply)
	return capMsg.getCapture().protomsg
}

func (rcs *readCapStream) Err() error {
	return rcs.lastErr
}

func (rcs *readCapStream) Close() {
	close(rcs.cancel)
	rcs.wp.Done()
}

func newReadCapStream(parStream *sessionStream, pcancel chan bool, rf ReadFilter) *readCapStream {
	r := &readCapStream{sessionStream: parStream}
	r.cancel = make(chan bool)
	r.lastRep = nil
	r.lastErr = nil

	ctx, cf := context.WithCancel(context.Background())

	go func(par chan bool, child chan bool, cf context.CancelFunc) {
		select {
		case <-par:
			break
		case <-child:
			break
		}
		cf()
	}(pcancel, r.cancel, cf)

	ex := newSessionExecutor(r.db.Db(), r.oper)
	r.dbResp = getCaptureBinaryStream(ctx, ex, newGetCapMessage(rf))
	return r
}
