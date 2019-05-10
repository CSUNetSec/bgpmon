package db

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/CSUNetSec/bgpmon/config"
	"github.com/CSUNetSec/bgpmon/util"
)

func init() {
	util.DisableLogging()
}

func loadSessionConfiger() (config.SessionConfiger, error) {
	fd, err := os.Open("../docs/bgpmond_config.toml")
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	config, err := config.NewConfig(fd)
	if err != nil {
		return nil, err
	}

	sc, err := config.GetSessionConfigWithName("LocalPostgres")
	if err != nil {
		return nil, err
	}
	return sc, nil
}

func openTestSession(wc int) (*Session, error) {
	sc, err := loadSessionConfiger()
	if err != nil {
		return nil, err
	}

	session, err := NewSession(sc, "test-session", wc)
	if err != nil {
		return nil, err
	}
	return session, nil
}

func TestSessionOpenClose(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	session, err := openTestSession(1)
	if err != nil {
		t.Fatal(err)
	}

	err = session.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSessionSetWorkers(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	workers := 8

	session, err := openTestSession(workers)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	if session.GetMaxWorkers() != workers {
		t.Fatalf("Expected: %d, Got: %d", workers, session.GetMaxWorkers())
	}

}

func TestSessionDefaultWorkers(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	sc, err := loadSessionConfiger()
	if err != nil {
		t.Fatal(err)
	}
	defWC := sc.GetWorkerCt()
	if defWC == 0 {
		defWC = 1
	}

	session, err := NewSession(sc, "test-session", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	if session.GetMaxWorkers() != defWC {
		t.Fatalf("Expected: %d, Got: %d", defWC, session.GetMaxWorkers())
	}
}

func TestOpenWriteStreams(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	session, err := openTestSession(4)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	var mux sync.Mutex
	openStreams := 0
	for i := 0; i < 8; i++ {
		ws, err := session.OpenWriteStream(SessionWriteCapture)
		if err != nil {
			t.Fatal(err)
		}
		mux.Lock()
		openStreams++
		if openStreams > 4 {
			t.Fatalf("Opened more streams than allowed. Allowed: %d, Opened: %d", 4, openStreams)
		}
		mux.Unlock()
		go func(w WriteStream) {
			time.Sleep(250 * time.Millisecond)
			mux.Lock()
			openStreams--
			mux.Unlock()
			w.Close()
		}(ws)
	}
}
