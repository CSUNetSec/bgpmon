// The worker manager is responsible for the lifetime and command dispatch
// of worker goroutines that work on the database. It has a main goroutine
// that is spawned when Run() is called and it accepts command on a channel.
// it responds on a reply channel. It is terminated by calling Stop(). The
// manager acts as a synchronization point because some commands can be executed
// potentially in parallel (like database writes of rows in existing tables), but
// others like database schema changes are better performed in an isolated fashion.
package db

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"sync"
)

var (
	worklogger = logrus.WithField("sys", "workerManager")
)

const (
	cmdNormal = iota //normal means that if there are free workers in the pool it will be scheduled
	cmdAtomic        //atomic means that when it received no more ops will run until it is done
	cmdTerm          //term terminates the worker manager
	cmdDrain         //finish all remaining tasks and don't accept anything other thatn cmdTerm and cmdStart
	cmdStart         //start accepting tasks
)

const (
	replyOk = iota
	replyErr
)

type workerCmd struct {
	cmdType int
	sin     sqlIn
	fun     workFunc
}

type workerReply struct {
	replyType int
	err       error
	sout      sqlOut
}

type workerMgr struct {
	inCmd      chan workerCmd
	outCmd     chan workerReply
	numworkers int
	workerwg   *sync.WaitGroup
}

// Run fires up a new goroutine to handle the database workers.
func (w workerMgr) Run() {
	go func() {
		worklogger.Info("starting worker manager for db")
		for {
			select {
			case icmd := <-w.inCmd:
				workcmdlogger := worklogger.WithField("cmd", icmd)
				workcmdlogger.Info("received command")
				switch icmd.cmdType {
				case cmdTerm:
					workcmdlogger.Info("terminating")
					w.outCmd <- workerReply{replyType: replyOk}
					return
				default:
					workcmdlogger.Error("unhandled command")
					w.outCmd <- workerReply{replyType: replyErr, err: errors.New("unhandled command")}
				}
			}
		}
		worklogger.Info("stopping worker manager for db")
	}()
}

// NewWorkerMgr returns a new worker manager struct.
func NewWorkerMgr(num int) workerMgr {
	return workerMgr{
		inCmd:      make(chan workerCmd),
		outCmd:     make(chan workerReply),
		numworkers: num,
		workerwg:   &sync.WaitGroup{},
	}
}

func (wm workerMgr) sendCmdReadReply(cmd workerCmd) {
	wm.inCmd <- cmd
	ret := <-wm.outCmd
	if ret.replyType == replyErr {
		worklogger.Errorf("worker manager replied with error: %s\n", ret.err)
	}
}
