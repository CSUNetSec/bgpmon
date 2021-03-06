package db

import (
	"fmt"
	"sync"

	"github.com/CSUNetSec/bgpmon/util"
)

const (
	bufferSize = 40
)

// writeCapStream is the WriteStream for BGP captures.
// Internally it synchronizes with the schema manager and keeps open buffers
// for efficient writes.
type writeCapStream struct {
	*sessionStream
	req    chan CommonMessage
	resp   chan CommonReply
	cancel chan bool

	ex       util.AtomicSQLExecutor
	buffers  map[string]util.SQLBuffer
	cache    tableCache
	daemonWG sync.WaitGroup
}

// newWriteCapStream returns a newly allocated writeCapStream.
func newWriteCapStream(baseStream *sessionStream, pCancel chan bool) (*writeCapStream, error) {
	w := &writeCapStream{sessionStream: baseStream, daemonWG: sync.WaitGroup{}}

	parentCancel := pCancel
	childCancel := make(chan bool)
	daemonCancel := make(chan bool)
	// If the parent requests a close, and the routine isn't already closed,
	// pass that on to the child.
	go func(par, child, daemon chan bool) {
		select {
		case <-par:
			daemon <- false
		case <-child:
			daemon <- true
		}
		close(daemon)
	}(parentCancel, childCancel, daemonCancel)
	w.cancel = childCancel

	w.req = make(chan CommonMessage)
	// This needs to have a buffer of 1 so the daemon can send back a response
	// when it's cancelled, and doesn't have to wait for the next request.
	w.resp = make(chan CommonReply, 1)
	w.buffers = make(map[string]util.SQLBuffer)
	w.cache = newNestedTableCache(baseStream.schema)

	ctxTx, err := newCtxExecutor(w.db)
	if err != nil {
		dbLogger.Errorf("Error opening ctxTx executor: %s", err)
		close(w.cancel)
		return nil, err
	}
	w.ex = ctxTx

	w.daemonWG.Add(1)
	go w.listen(daemonCancel)
	return w, nil
}

// Write performs a type of send on the sessionstream with an arbitrary argument.
// WARNING, sending after a close will cause a panic, and may hang.
func (w *writeCapStream) Write(arg interface{}) error {
	cap := arg.(*Capture)

	// Check our local cache first, otherwise contact schemaMgr.
	table, err := w.cache.LookupTable(cap.ColIP, cap.Timestamp)
	if err != nil {
		return dbLogger.Errorf("failed to get table from cache: %s", err)
	}

	capMsg := newCaptureMessage(table, cap)
	// Make sure this message uses the same tables as the schema
	w.schema.setMessageTables(capMsg)

	w.req <- capMsg
	resp, ok := <-w.resp
	if !ok {
		return fmt.Errorf("response channel closed")
	}

	return resp.Error()
}

// Flush is called when a stream finishes successfully.
// It flushes all remaining buffers.
func (w *writeCapStream) Flush() error {
	dbLogger.Infof("Flushing stream")
	for key := range w.buffers {
		err := w.buffers[key].Flush()
		if err != nil {
			dbLogger.Errorf("writeCapStream failed to flush buffer: %s", err)
		}
	}
	return w.ex.Commit()
}

//Cancel is used when there is an error on the client-side,
//called to rollback all executed queries
func (w *writeCapStream) Cancel() {
	dbLogger.Infof("Cancelling stream")
	for key := range w.buffers {
		w.buffers[key].Clear()
	}

	if err := w.ex.Rollback(); err != nil {
		dbLogger.Errorf("Error rolling back stream: %s", err)
	}
}

//Close is only for a normal close operation. A cancellation
//can only be done by Closing the parent session while
//the stream is still running
//This should be called by the same goroutine as the one calling Write.
func (w *writeCapStream) Close() {
	dbLogger.Infof("Closing session stream")
	close(w.cancel)
	close(w.req)
	w.daemonWG.Wait()
	w.wp.Done()
	return
}

// This is the writeCapStream goroutine
// This function is a little bit tricky, because a stream needs to be closable
// from two different directions.
// 1. A normal close. This is when a client calls Close on the writeCapStream
//	  after it is done communicating with it.
//		We can assume that nothing more will come in on the request channel.
// 2. A session close. This occurs on an unexpected shutdown, such as ctrl-C.
//		A client may try to send requests to this after it has been closed. It
//		should return that the stream has been closed before shutting down
//		completely.
func (w *writeCapStream) listen(cancel chan bool) {
	defer dbLogger.Infof("WriteCapStream closed successfully")
	defer close(w.resp)
	defer w.daemonWG.Done()

	for {
		select {
		case normal, open := <-cancel:
			// If this is closed, it can spam this select case. Just ignore it
			// if it happens. It will always receive a real value before it's
			// closed.
			if !open {
				continue
			}

			// If it was an abnormal closing, the client may, or may not, expect
			// another value from a call to Write(). This channel has a buffer of
			// 1 so this won't block, and the value can be received soon.
			if !normal {
				w.resp <- newReply(fmt.Errorf("writeCapStream cancelled"))
			}
			return
		case val, ok := <-w.req:
			// The w.req channel might see it's close before the cancel channel.
			// If that happens, this will add an empty sqlIn to the buffer. If
			// it has been closed, that's the same as a normal closure, and this
			// can just return
			if ok {
				capMsg := val.(*captureMessage)
				tName := capMsg.getTableName()

				buf, ok := w.buffers[tName]
				if !ok {
					buf = util.NewInsertBuffer(w.ex, bufferSize, true)
					w.buffers[tName] = buf
				}

				rep := insertCapture(newSessionExecutor(buf, w.oper), capMsg)
				w.resp <- rep
			} else {
				return
			}
		}
	}
}

// writeEntityStream is a Write Stream that writes Entity structs into the database.
type writeEntityStream struct {
	*sessionStream

	cancel chan bool
	done   bool
	ex     util.AtomicSQLExecutor
}

// Write will panic if ent is not an Entity struct.
func (es *writeEntityStream) Write(e interface{}) error {
	entity := e.(*Entity)

	entMsg := newEntityMessage(entity)
	// Make sure this uses the same tables as the schema
	es.schema.setMessageTables(entMsg)

	rep := insertEntity(newSessionExecutor(es.ex, es.oper), entMsg)

	return rep.Error()
}

func (es *writeEntityStream) Flush() error {
	if es.done {
		return nil
	}

	es.done = true
	return es.ex.Commit()
}

func (es *writeEntityStream) Cancel() {
	if es.done {
		return
	}

	err := es.ex.Rollback()
	if err != nil {
		dbLogger.Errorf("Error rolling back writeEntityStream write: %s", err)
	}
	es.done = true
}

func (es *writeEntityStream) Close() {
	close(es.cancel)
	es.wp.Done()
}

func (es *writeEntityStream) waitForCancel(parent chan bool) {
	select {
	case <-parent:
		// Parent cancel by closing the session
		es.Cancel()
	case <-es.cancel:
		// Child cancel
		es.Cancel()
	}
}

func newWriteEntityStream(baseStream *sessionStream, pcancel chan bool) (*writeEntityStream, error) {
	es := &writeEntityStream{sessionStream: baseStream}
	ctxEx, err := newCtxExecutor(baseStream.db)
	if err != nil {
		return nil, err
	}

	es.ex = ctxEx
	es.cancel = make(chan bool)
	go es.waitForCancel(pcancel)

	return es, nil
}
