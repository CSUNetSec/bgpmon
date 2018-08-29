package util

import (
	"sync"
)

type WorkerPool struct {
	max      int
	active   int
	req      chan bool
	done     chan bool
	close    chan bool
	workerWg *sync.WaitGroup
	daemonWg *sync.WaitGroup
}

func NewWorkerPool(ct int) *WorkerPool {
	wp := &WorkerPool{max: ct, active: 0, workerWg: &sync.WaitGroup{}, daemonWg: &sync.WaitGroup{}}
	wp.req = make(chan bool)
	wp.done = make(chan bool, ct)
	wp.close = make(chan bool)

	wp.daemonWg.Add(1)
	go wp.daemon()
	return wp
}

func (wp *WorkerPool) Add() {
	wp.workerWg.Add(1)
	<-wp.req
}

func (wp *WorkerPool) Done() {
	wp.done <- true
	wp.workerWg.Done()
}

func (wp *WorkerPool) Close() bool {
	wp.workerWg.Wait()
	wp.close <- true
	wp.daemonWg.Wait()

	return true
}

func (wp *WorkerPool) daemon() {
	defer wp.daemonWg.Done()

	for {
		select {
		case <-wp.close:
			return
		default:
		}

		if wp.active == wp.max {
			select {
			case <-wp.done:
				wp.active--
			}
		} else {
			select {
			case <-wp.done:
				wp.active--
			case wp.req <- true:
				wp.active++
			}
		}
	}
}
