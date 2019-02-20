package util

import (
	"sync"
	"testing"
	"time"
)

const (
	WPSleep       = 2 * time.Second
	WPMaxRunning  = 10
	WPMaxLaunched = 50
)

func TestWorkerPool(t *testing.T) {

	t.Logf("Testing worker pool with %d goroutines\n", WPMaxRunning)
	wp := NewWorkerPool(WPMaxRunning)

	mx := &sync.Mutex{}
	active := 0
	finished := 0

	for i := 0; i < WPMaxLaunched; i++ {
		wp.Add()
		go func() {
			mx.Lock()
			active++
			if active > WPMaxRunning {
				t.Errorf("Max running of %d exceeded\n", WPMaxRunning)
			}
			mx.Unlock()

			time.Sleep(WPSleep)

			mx.Lock()
			active--
			finished++
			mx.Unlock()

			wp.Done()
		}()
	}

	closed := wp.Close()
	if finished < WPMaxLaunched {
		t.Errorf("Only %d/%d goroutines allowed to finish\n", finished, WPMaxLaunched)
	}

	if !closed {
		t.Errorf("Failed to close daemon\n")
	}
}

func TestWorkerPoolClose(t *testing.T) {

	finished := false
	wp := NewWorkerPool(1)
	wp.Add()
	go func() {
		defer wp.Done()
		time.Sleep(WPSleep)
		finished = true
	}()

	wp.Close()
	if !finished {
		t.Errorf("Worker pool closed before individual workers finished")
	}
}
