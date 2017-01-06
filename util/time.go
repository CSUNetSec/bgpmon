//Utility functions for the handling of time related tasks
package util

import (
	"errors"
	"time"
)

var (
	errtimeorder = errors.New("start time is later than end time")
	errzeroquant = errors.New("Quantum can't be zero")
)

type TimeRangeGenerator struct {
	start   time.Time
	end     time.Time
	quantum time.Duration
	cur     time.Time
	starts  *time.Time
}

func NewTimeRangeGenerator(start, end time.Time, quantum time.Duration) (*TimeRangeGenerator, error) {
	if start.After(end) {
		return nil, errtimeorder
	}
	if quantum == time.Duration(0) {
		return nil, errzeroquant
	}
	if quantum < 0 {
		return &TimeRangeGenerator{end, start, quantum, end, &end}, nil
	}
	return &TimeRangeGenerator{start, end, quantum, start, &start}, nil
}

func (t TimeRangeGenerator) Next() bool {
	if t.quantum > 0 {
		if t.cur.Add(t.quantum).After(t.end) {
			return false
		}
		return true
	}
	if t.cur.Add(t.quantum).Before(t.end) {
		return false
	}
	return true
}

func (t *TimeRangeGenerator) DatePair() (A, B time.Time) {
	A, B = t.cur, t.cur.Add(t.quantum)
	t.cur = B
	return
}

func (t *TimeRangeGenerator) Reset() {
	t.cur = *t.starts
}
