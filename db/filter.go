package db

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/CSUNetSec/bgpmon/util"
)

const (
	// AnyCollector is a wild card for collectors in the DB.
	AnyCollector = "%"
)

type readFilter interface {
	getWhereClause() string
}

// FilterOptions is an empty interface. Only the implementations of it
// are important.
type FilterOptions interface{}

// CaptureFilterOptions contains the options to filter by capture messages.
type CaptureFilterOptions struct {
	collector      string
	span           util.Timespan
	hasExtraFilter bool

	origin   int
	advPrefs []*net.IPNet
}

// SetOrigin filters by the provided origin autonomous system (AS).
func (cfo *CaptureFilterOptions) SetOrigin(as int) {
	cfo.hasExtraFilter = true
	cfo.origin = as
}

// AllowAdvPrefixes adds the provided prefixes to a list of prefixes to filter
// by. If any prefix on that list appears, the Capture will pass the filter.
func (cfo *CaptureFilterOptions) AllowAdvPrefixes(prefs ...*net.IPNet) {
	cfo.hasExtraFilter = true
	cfo.advPrefs = append(cfo.advPrefs, prefs...)
}

// NewCaptureFilterOptions returns a FilterOptions interface for filtering
// captures.
func NewCaptureFilterOptions(collector string, start time.Time, end time.Time) *CaptureFilterOptions {
	cfo := &CaptureFilterOptions{
		collector:      collector,
		span:           util.Timespan{Start: start, End: end},
		hasExtraFilter: false,
		origin:         -1,
		advPrefs:       nil,
	}
	return cfo
}

// DefaultCaptureFilterOptions returns a CaptureFilterOptions with any collector, and
// time.Now() as the start and end date. This is unlikely to return any captures.
func DefaultCaptureFilterOptions() *CaptureFilterOptions {
	now := time.Now()
	return NewCaptureFilterOptions(AnyCollector, now, now)
}

type captureFilter struct {
	*CaptureFilterOptions
}

func (cf *captureFilter) getWhereClause() string {
	if !cf.hasExtraFilter {
		return ""
	}

	crossJoin := ""
	var conditions []string

	if cf.advPrefs != nil {
		crossJoin = "CROSS JOIN UNNEST(adv_prefixes) as advPrefix"
		var prefStr []string
		for _, v := range cf.advPrefs {
			prefStr = append(prefStr, fmt.Sprintf("'%s'", v.String()))
		}
		advCond := fmt.Sprintf("advPrefix IN (%s)", strings.Join(prefStr, ","))
		conditions = append(conditions, advCond)
	}

	if cf.origin != -1 {
		conditions = append(conditions, fmt.Sprintf("origin_as = %d", cf.origin))
	}

	return fmt.Sprintf("%s WHERE %s", crossJoin, strings.Join(conditions, " AND "))
}

// newCaptureFilter returns a readFilter for captures based on the provided options.
// The options must be a *CaptureFilterOptions. If opts is nil, it uses default options.
// It can't leave the options as nil because the fields must be populated with something.
func newCaptureFilter(opts FilterOptions) (*captureFilter, error) {
	if opts == nil {
		return &captureFilter{CaptureFilterOptions: DefaultCaptureFilterOptions()}, nil
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
