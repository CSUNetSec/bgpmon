package util

import (
	"sync"
)

type WorkerPool struct {
	max    int
	active int
	req    chan bool
	done   chan bool
	close  chan bool
	wg     *sync.WaitGroup
}

func NewWorkerPool(ct int) *WorkerPool {
	wp := &WorkerPool{max: ct, active: 0, wg: &sync.WaitGroup{}}
	wp.req = make(chan bool)
	wp.done = make(chan bool, ct)
	wp.close = make(chan bool)

	go wp.daemon()
	return wp
}

func (wp *WorkerPool) Add() {
	wp.wg.Add(1)
	<-wp.req
}

func (wp *WorkerPool) Done() {
	wp.done <- true
	wp.wg.Done()
}

func (wp *WorkerPool) Close() {
	wp.wg.Wait()
	wp.close <- true
}

func (wp *WorkerPool) daemon() {
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
