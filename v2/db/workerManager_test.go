package db

import (
	"testing"
)

var (
	normcmd = workerCmd{cmdType: cmdNormal}
	invcmd  = workerCmd{cmdType: 89}
	termcmd = workerCmd{cmdType: cmdTerm}
)

func TestWorkMgrCommands(t *testing.T) {
	wm := NewWorkerMgr()
	wm.Run()
	wm.sendCmdReadReply(normcmd)
	wm.sendCmdReadReply(invcmd)
	wm.sendCmdReadReply(termcmd)
}
