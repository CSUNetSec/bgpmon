package db

import (
	"fmt"
	"net"
	"sync"

	"github.com/CSUNetSec/bgpmon/util"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/lib/pq"
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
	wr := arg.(*pb.WriteRequest)
	mTime, cIP, err := util.GetTimeColIP(wr.GetBgpCapture())
	if err != nil {
		dbLogger.Errorf("failed to get Collector IP:%v", err)
		return err
	}
	// Check our local cache first, otherwise contact schemaMgr.
	table, err := w.cache.LookupTable(cIP, mTime)
	if err != nil {
		return dbLogger.Errorf("failed to get table from cache: %s", err)
	}
	w.req <- newCaptureMessage(table, wr)
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
				w.resp <- newReply(w.addToBuffer(val))
			} else {
				return
			}
		}
	}
}

func (w *writeCapStream) addToBuffer(msg CommonMessage) error {
	cMsg := msg.(captureMessage)

	tName := cMsg.getTableName()
	if _, ok := w.buffers[tName]; !ok {
		dbLogger.Infof("Creating new buffer for table: %s", tName)
		stmt := fmt.Sprintf(w.oper.getQuery(insertCaptureTableOp), tName)
		w.buffers[tName] = util.NewInsertBuffer(w.ex, stmt, bufferSize, 9, true)
	}
	buf := w.buffers[tName]
	// This actually returns a WriteRequest, not a BGPCapture, but all the utility functions were built around
	// WriteRequests
	cap := cMsg.getCapture()

	timestamp, colIP, _ := util.GetTimeColIP(cap)
	peerIP, err := util.GetPeerIP(cap)
	if err != nil {
		dbLogger.Infof("Unable to parse peer ip, ignoring message")
		return nil
	}

	asPath, _ := util.GetASPath(cap) // Ignoring the error here as this message could only have withdraws.
	nextHop, err := util.GetNextHop(cap)
	if err != nil {
		nextHop = net.IPv4(0, 0, 0, 0)
	}
	origin := 0
	if len(asPath) != 0 {
		origin = asPath[len(asPath)-1]
	} else {
		origin = 0
	}
	// Here if it errors and the return is nil, PrefixToPQArray should leave it and the schema should insert the default
	advertised, _ := util.GetAdvertisedPrefixes(cap)
	withdrawn, _ := util.GetWithdrawnPrefixes(cap)
	protoMsg := util.GetProtoMsg(cap)

	advArr := util.PrefixesToPQArray(advertised)
	wdrArr := util.PrefixesToPQArray(withdrawn)

	return buf.Add(timestamp, colIP.String(), peerIP.String(), pq.Array(asPath), nextHop.String(), origin, advArr, wdrArr, protoMsg)
}
