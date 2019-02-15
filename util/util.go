package util

import (
	"sync"
)

//WorkerPool is a struct that manages the amount of goroutines by blocking on and add
type WorkerPool struct {
	max      int
	active   int
	req      chan bool
	done     chan bool
	close    chan bool
	workerWg *sync.WaitGroup
	daemonWg *sync.WaitGroup
}

// NewWorkerPool returns a worker pool with ct as the maximum number of goroutines
func NewWorkerPool(ct int) *WorkerPool {
	wp := &WorkerPool{max: ct, active: 0, workerWg: &sync.WaitGroup{}, daemonWg: &sync.WaitGroup{}}
	wp.req = make(chan bool)
	wp.done = make(chan bool, ct)
	wp.close = make(chan bool)

	wp.daemonWg.Add(1)
	go wp.daemon()
	return wp
}

// Add adds a worker to the pool
func (wp *WorkerPool) Add() {
	wp.workerWg.Add(1)
	<-wp.req
}

// Done signifies a worker is finished
func (wp *WorkerPool) Done() {
	wp.done <- true
	wp.workerWg.Done()
}

// Close waits for all workers to be finished and closes the daemon goroutine
func (wp *WorkerPool) Close() bool {
	wp.workerWg.Wait()
	wp.close <- true
	wp.daemonWg.Wait()

	return true
}

func (wp *WorkerPool) daemon() {
	defer wp.daemonWg.Done()

	for {
		if wp.active == wp.max {
			select {
			case <-wp.done:
				wp.active--
			case <-wp.close:
				return
			}
		} else {
			select {
			case <-wp.done:
				wp.active--
			case wp.req <- true:
				wp.active++
			case <-wp.close:
				return
			}
		}
	}
}
