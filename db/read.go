package db

import (
	"context"
)

type readCapStream struct {
	*sessionStream

	lastRep CommonReply
	lastErr error

	dbResp chan CommonReply
	cancel chan bool
}

func (rcs *readCapStream) Read() bool {
	msg, ok := <-rcs.dbResp

	if !ok {
		rcs.lastErr = nil
		return false
	}

	if msg.Error() != nil {
		rcs.lastErr = msg.Error()
		return false
	}

	rcs.lastRep = msg
	rcs.lastErr = nil
	return true
}

func (rcs *readCapStream) Data() interface{} {
	if rcs.lastRep == nil {
		return nil
	}

	capMsg := rcs.lastRep.(getCapReply)
	return capMsg.getCapture()
}

func (rcs *readCapStream) Bytes() []byte {
	if rcs.lastRep == nil {
		return nil
	}

	capMsg := rcs.lastRep.(getCapReply)
	return capMsg.getCapture().protoMsg
}

func (rcs *readCapStream) Err() error {
	return rcs.lastErr
}

func (rcs *readCapStream) Close() {
	close(rcs.cancel)
	rcs.wp.Done()
}

func newReadCapStream(parStream *sessionStream, pCancel chan bool, rf ReadFilter) *readCapStream {
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
	}(pCancel, r.cancel, cf)

	ex := newSessionExecutor(r.db.DB(), r.oper)
	r.dbResp = getCaptureBinaryStream(ctx, ex, newGetCapMessage(rf))
	return r
}

type readPrefixStream struct {
	*sessionStream

	lastRep CommonReply
	lastErr error

	dbResp chan CommonReply
	cancel chan bool
}

func (rps *readPrefixStream) Read() bool {
	msg, ok := <-rps.dbResp

	if !ok {
		rps.lastErr = nil
		return false
	}

	if msg.Error() != nil {
		rps.lastErr = msg.Error()
		return false
	}

	rps.lastRep = msg
	rps.lastErr = nil
	return true

}

func (rps *readPrefixStream) Data() interface{} {
	if rps.lastRep == nil {
		return nil
	}

	prefRep := rps.lastRep.(*getPrefixReply)
	return prefRep.getPrefix()
}

func (rps *readPrefixStream) Bytes() []byte {
	if rps.lastRep == nil {
		return nil
	}

	prefRep := rps.lastRep.(*getPrefixReply)
	return []byte(prefRep.getPrefix().String())
}

func (rps *readPrefixStream) Err() error {
	return rps.lastErr
}

func (rps *readPrefixStream) Close() {
	close(rps.cancel)
	rps.wp.Done()
}

func newReadPrefixStream(parStream *sessionStream, pCancel chan bool, rf ReadFilter) *readPrefixStream {
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
	}(pCancel, r.cancel, cf)

	ex := newSessionExecutor(r.db.DB(), r.oper)
	r.dbResp = getPrefixStream(ctx, ex, newGetCapMessage(rf))
	return r
}

type readEntityStream struct {
	*sessionStream

	lastRep *Entity
	lastErr error

	cancel chan bool
	dbResp chan CommonReply
}

func (es *readEntityStream) Read() bool {
	rep, ok := <-es.dbResp
	if !ok {
		es.lastErr = io.EOF
		return false
	}

	if rep.Error() != nil {
		es.lastErr = rep.Error()
		return false
	}

	entRep := rep.(*entityReply)
	es.lastRep = entRep.getEntity()
	return true
}

func (es *readEntityStream) Data() interface{} {
	return es.lastRep
}

func (es *readEntityStream) Bytes() []byte {
	return []byte{}
}

func (es *readEntityStream) Err() error {
	return es.lastErr
}

func (es *readEntityStream) Close() {
	close(es.cancel)
	es.wp.Done()
}

func newReadEntityStream(baseStream *sessionStream, pCancel chan bool, rf ReadFilter) *readEntityStream {
	es := &readEntityStream{sessionStream: baseStream}
	es.cancel = make(chan bool)
	es.lastRep = nil
	es.lastErr = nil

	ctx, cf := context.WithCancel(context.Background())
	go func(par chan bool, child chan bool, cf context.CancelFunc) {
		select {
		case <-par:
			break
		case <-child:
			break
		}
		cf()
	}(pCancel, es.cancel, cf)

	ex := newSessionExecutor(es.db.DB(), es.oper)
	es.dbResp = getEntityStream(ctx, ex, newFilterMessage(rf))
	return es
}
