package util

import (
	"database/sql"
	"fmt"
	"github.com/CSUNetSec/bgpmon/v2/config"
	"sort"
	"time"
)

func SumNodeConfs(confnodes, dbnodes map[string]config.NodeConfig) map[string]config.NodeConfig {
	ret := make(map[string]config.NodeConfig)
	for k1, v1 := range confnodes {
		ret[k1] = v1 //if it exists in config, prefer config
	}
	for k2, v2 := range dbnodes {
		if _, ok := confnodes[k2]; ok { // exists in config, so ignore it
			continue
		}
		//does not exist in config, so add it in ret as it is in the db
		ret[k2] = v2
	}
	return ret
}

//this struct is the element of an ordered array
//that will be used to name tables in the db and also
//refer to them when they are open.
type collectorDateString struct {
	colName   string
	startDate time.Time
	duration  time.Duration
}

func (c collectorDateString) GetNameDateStr() string {
	return fmt.Sprintf("%s-%s", c.colName, c.startDate.Format("2006-01-02-15-04-05"))
}

func NewCollectorDateString(name string, sd time.Time, dur time.Duration) *collectorDateString {
	return &collectorDateString{
		colName:   name,
		startDate: sd,
		duration:  dur,
	}
}

//collector-date strings ordered by their starting date.
type CollectorsByNameDate []collectorDateString

//Len implementation for sort interface
func (c CollectorsByNameDate) Len() int {
	return len(c)
}

//Swap implementation for sort interface
func (c CollectorsByNameDate) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

//Less implementation for sort interface. uses the stable
//sort attribute to do two keys. first by name then by date.
func (c CollectorsByNameDate) Less(i, j int) bool {
	if c[i].colName < c[j].colName {
		return true
	} else if c[i].colName > c[j].colName {
		return false
	}
	return c[i].startDate.Before(c[j].startDate)
}

//this will return the index and if the name and date are in the slice. caller has to check existence.
func (c CollectorsByNameDate) ColNameDateInSlice(colname string, date time.Time) (int, bool) {
	//find a possible index
	ind := sort.Search(c.Len(), func(i int) bool {
		return c[i].colName == colname && (c[i].startDate.After(date) || c[i].startDate.Equal(date))
	})
	//validate that the name is the same
	if c[ind].colName != colname {
		return 0, false
	}
	//catch exact same date
	if c[ind].startDate.Equal(date) {
		return ind, true
	}
	//catch the normal case where it is after the startdate, and before the startdate+duration
	if c[ind].startDate.Before(date) && date.Before(c[ind].startDate.Add(c[ind].duration)) {
		return ind, true
	}
	return 0, false
}

// This is a wrapper around sql.Tx, sql.Db, and others we implement
type SqlExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}
