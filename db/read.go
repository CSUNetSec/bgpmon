package db

import (
	"context"
	"fmt"
	"io"
)

type readCapStream struct {
	*sessionStream
	dbResp chan CommonReply
	cancel chan bool
	cf     context.CancelFunc
}

func (rcs *readCapStream) Read() (interface{}, error) {
	select {
	case <-rcs.cancel:
		// Cancellation happened
		return nil, fmt.Errorf("Parent session cancelled")
	case msg, ok := <-rcs.dbResp:
		// More data from the db
		if !ok {
			return nil, io.EOF
		}
		capMsg := msg.(getCapReply)

		return capMsg.getCapture(), capMsg.Error()
	}
}

func (rcs *readCapStream) Close() {
	rcs.cf()
	rcs.wp.Done()
}

func newReadCapStream(parStream *sessionStream, pcancel chan bool, rf ReadFilter) *readCapStream {
	r := &readCapStream{sessionStream: parStream, cancel: pcancel}
	ctx, cf := context.WithCancel(context.Background())
	r.cf = cf

	ex := newSessionExecutor(r.db.Db(), r.oper)
	r.dbResp = getCaptureBinaryStream(ctx, ex, newGetCapMessage(rf))
	return r
}
