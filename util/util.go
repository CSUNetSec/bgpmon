// Package util defines miscellaneous functions used in multiple parts of bgpmon
// or other projects
package util

import (
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
)

var (
	// ErrOpt is returned by StringToOptMap on failure
	ErrOpt = errors.New("error parsing options")
)

// Timespan represents a start and end time.
type Timespan struct {
	Start time.Time
	End   time.Time
}

// Contains returns true if t is within [start, end)
func (ts Timespan) Contains(t time.Time) bool {
	if ts.Start.Equal(t) {
		return true
	}

	if ts.Start.Before(t) && t.Before(ts.End) {
		return true
	}
	return false
}

// GetTimeouter is an interface to describe anything that expires after a
// certain amount of time
type GetTimeouter interface {
	GetTimeout() time.Duration
}

// StringToOptMap is a function that will turn a string in the form "-opt1 val1 -opt2 val2" to
// a map[string]string with key:values like opt1:val1, opt2:val2. It will return an error in the
// case of a malformed string. For now only options with values are supported, so the input
// string must be split in an even number of parts.
func StringToOptMap(in string) (map[string]string, error) {
	// first split the string in spaces
	ret := make(map[string]string)
	if in == "" {
		return ret, nil
	}

	inParts := strings.Fields(in)
	if len(inParts)%2 != 0 {
		return nil, ErrOpt
	}
	for i := range inParts {
		if i%2 == 0 && len(inParts) > i+1 { // iterate on pairs
			optStr := inParts[i]
			optVal := inParts[i+1]
			if !strings.HasPrefix(optStr, "-") {
				return nil, ErrOpt
			}
			ret[optStr[1:]] = optVal
		}
	}
	return ret, nil
}

// OptMapToString is a function that turns a map[string]string to a string like "-key1 val1 -key2 val2".
// It should be the reverse operation of StringToOptMap. This function only guarantees the correct
// pairings, not the original order of the pairings when the map was created.
func OptMapToString(in map[string]string) string {
	var retBuilder strings.Builder
	for k, v := range in {
		retBuilder.WriteString(fmt.Sprintf("-%s %s ", k, v))
	}
	return retBuilder.String()
}

// CheckForKeys checks a map[string]string for the existence of all string keys provided.
func CheckForKeys(in map[string]string, keys ...string) bool {
	for _, k := range keys {
		_, ok := in[k]
		if !ok {
			return false
		}
	}
	return true
}
