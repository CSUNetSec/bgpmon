// Package util defines miscellaneous functions used in multiple parts of bgpmon
// or other projects
package util

import (
	"fmt"
	"github.com/pkg/errors"
	"strings"
	"sync"
	"time"
)

var (
	// ErrOpt is returned by StringToOptMap on failure
	ErrOpt = errors.New("Error parsing options")
)

//GetTimeouter is a generic interface for things to return timeouts
type GetTimeouter interface {
	GetTimeout() time.Duration
}

//WorkerPool is a type that limits the number of running goroutines
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

// Close waits for all workers to be finished
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

// StringToOptMap is a function that will turn a string in the form "-opt1 val1 -opt2 val2" to
// a map[string]string with key:values like opt1:val1, opt2:val2. In case of a malformed string it errors.
// For now only options with values are supported. therefore the string must be split in an even
// number of parts
func StringToOptMap(in string) (map[string]string, error) {
	//first split the string in spaces
	ret := make(map[string]string)
	inparts := strings.Fields(in)
	if len(inparts)%2 != 0 {
		return nil, ErrOpt
	}
	for i := range inparts {
		if i%2 == 0 && len(inparts) > i+1 { //iterate on pairs
			optstr := inparts[i]
			optval := inparts[i+1]
			if !strings.HasPrefix(optstr, "-") {
				return nil, ErrOpt
			}
			ret[optstr[1:]] = optval
		}
	}
	return ret, nil
}

// OptMapToString is a function that turns a map[string]string to a string like "-key1 val1 -key2 val2"
// It should be the reverse operation of StringToOptMap modulo preserving opt order
func OptMapToString(in map[string]string) string {
	var retbuild strings.Builder
	for k, v := range in {
		retbuild.WriteString(fmt.Sprintf("-%s %s ", k, v))
	}
	return retbuild.String()
}

// CheckForKeys checks a map[string]string for the existence of all string keys provided
func CheckForKeys(in map[string]string, keys ...string) bool {
	for _, k := range keys {
		if _, ok := in[k]; !ok {
			return false
		}
	}
	return true
}
