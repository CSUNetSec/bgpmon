package db

import (
	"context"
	"database/sql"
	"testing"
)

var (
	normcmd = workerCmd{cmdType: cmdNormal}
	invcmd  = workerCmd{cmdType: 89}
	termcmd = workerCmd{cmdType: cmdTerm}
)

func TestWorkMgrCommands(t *testing.T) {
	var tdb *sql.DB
	wm := NewWorkerMgr(10, tdb, context.Background())
	wm.Run()
	wm.sendCmdReadReply(normcmd)
	wm.sendCmdReadReply(invcmd)
	wm.sendCmdReadReply(termcmd)
}
