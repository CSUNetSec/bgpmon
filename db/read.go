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
	return capMsg.getCapture().protoMsg
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

type readPrefixStream struct {
	*sessionStream

	lastRep *CommonReply
	lastErr error

	dbResp chan CommonReply
	cancel chan bool
}

func (rps *readPrefixStream) Read() bool {
	msg, ok := <-rps.dbResp

	if !ok {
		rps.lastErr = io.EOF
		return false
	}

	if msg.Error() != nil {
		rps.lastErr = msg.Error()
		return false
	}

	rps.lastRep = &msg
	return true

}

func (rps *readPrefixStream) Data() interface{} {
	if rps.lastRep == nil {
		return nil
	}

	prefRep := (*rps.lastRep).(*getPrefixReply)
	return prefRep.getPrefix()
}

func (rps *readPrefixStream) Bytes() []byte {
	if rps.lastRep == nil {
		return nil
	}

	prefRep := (*rps.lastRep).(*getPrefixReply)
	return []byte(prefRep.getPrefix().String())
}

func (rps *readPrefixStream) Err() error {
	return rps.lastErr
}

func (rps *readPrefixStream) Close() {
	close(rps.cancel)
	rps.wp.Done()
}

func newReadPrefixStream(parStream *sessionStream, pcancel chan bool, rf ReadFilter) *readPrefixStream {
	r := &readPrefixStream{sessionStream: parStream}
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
	r.dbResp = getPrefixStream(ctx, ex, newGetCapMessage(rf))
	return r
}
