package util

import (
	"sync"
	"testing"
	"time"
)

const (
	WP_SLEEP        = 2 * time.Second
	WP_MAX_RUNNING  = 10
	WP_MAX_LAUNCHED = 50
)

func TestWorkerPool(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Logf("Testing worker pool with %d goroutines\n", WP_MAX_RUNNING)
	wp := NewWorkerPool(WP_MAX_RUNNING)

	mx := &sync.Mutex{}
	active := 0
	finished := 0

	for i := 0; i < WP_MAX_LAUNCHED; i++ {
		wp.Add()
		go func() {
			mx.Lock()
			active++
			mx.Unlock()

			if active > WP_MAX_RUNNING {
				t.Errorf("Max running of %d exceeded\n", WP_MAX_RUNNING)
			}

			time.Sleep(WP_SLEEP)

			mx.Lock()
			active--
			finished++
			mx.Unlock()

			wp.Done()
		}()
	}

	wp.Close()
	if finished < WP_MAX_LAUNCHED {
		t.Errorf("Only %d/%d goroutines allowed to finish\n", finished, WP_MAX_LAUNCHED)
	}

	if !wp.Closed() {
		t.Errorf("Failed to close worker pool daemon\n")
	}
}
