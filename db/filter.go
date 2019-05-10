package db

import (
	"fmt"
	"time"

	"github.com/CSUNetSec/bgpmon/util"
)

type readFilter interface {
	getWhereClause() string
}

// FilterOptions is an empty interface. Only the implementations of it
// are important.
type FilterOptions interface{}

// CaptureFilterOptions contains the options to filter by capture messages.
type CaptureFilterOptions struct {
	collector string
	span      util.Timespan
}

// NewCaptureFilterOptions returns a FilterOptions interface for filtering
// captures.
func NewCaptureFilterOptions(collector string, start time.Time, end time.Time) *CaptureFilterOptions {
	return &CaptureFilterOptions{collector: collector, span: util.Timespan{Start: start, End: end}}
}

type captureFilter struct {
	*CaptureFilterOptions
}

func (cf *captureFilter) getWhereClause() string {
	if cf.CaptureFilterOptions == nil {
		return ""
	}
	return ""
}

func newCaptureFilter(opts FilterOptions) (*captureFilter, error) {
	if opts == nil {
		return &captureFilter{CaptureFilterOptions: nil}, nil
	}

	var capOpts *CaptureFilterOptions
	switch opts.(type) {
	case *CaptureFilterOptions:
		capOpts = opts.(*CaptureFilterOptions)
		break
	default:
		return nil, fmt.Errorf("Need CaptureFilterOptions")
	}

	return &captureFilter{CaptureFilterOptions: capOpts}, nil
}

// EntityFilterOptions holds all the fields to filter entities.
type EntityFilterOptions struct {
	name string
}

type entityFilter struct {
	*EntityFilterOptions
}

func (e *entityFilter) getWhereClause() string {
	if e.EntityFilterOptions == nil {
		return ""
	}

	return fmt.Sprintf("WHERE name='%s'", e.name)
}

func newEntityFilter(fo FilterOptions) (*entityFilter, error) {
	if fo == nil {
		return &entityFilter{EntityFilterOptions: nil}, nil
	}

	var entOpts *EntityFilterOptions
	switch fo.(type) {
	case *EntityFilterOptions:
		entOpts = fo.(*EntityFilterOptions)
		break
	default:
		return nil, fmt.Errorf("Need CaptureFilterOptions")
	}

	return &entityFilter{EntityFilterOptions: entOpts}, nil
}

// NewEntityFilterOptions returns FilterOptions for an Entity
func NewEntityFilterOptions(name string) *EntityFilterOptions {
	return &EntityFilterOptions{name: name}
}
