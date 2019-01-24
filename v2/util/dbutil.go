package util

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/CSUNetSec/bgpmon/v2/config"
	"github.com/lib/pq"
	"net"
	"sort"
	"strings"
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
	endDate   time.Time
}

func (c collectorDateString) GetNameDateStr() string {
	return fmt.Sprintf("%s_%s", c.colName, c.startDate.Format("2006_01_02_15_04_05"))
}

func (c collectorDateString) GetNameDates() (string, string, time.Time, time.Time) {
	return c.colName, c.GetNameDateStr(), c.startDate, c.endDate
}

//this function gets the date for a new collector table to be added with the desired duration minutes
//and returns a string that should be the table name, the truncated time, and the end time.
func GetNodeTableNameDates(name string, stime time.Time, durmin int) (string, time.Time, time.Time) {
	dur := time.Duration(durmin) * time.Minute
	trunctime := stime.Truncate(dur).UTC()
	tname := fmt.Sprintf("%s_%s", name, trunctime.Format("2006_01_02_15_04_05"))
	return tname, trunctime, trunctime.Add(dur)
}

func NewCollectorDateString(name string, sd time.Time, ed time.Time) *collectorDateString {
	return &collectorDateString{
		colName:   name,
		startDate: sd,
		endDate:   ed,
	}
}

//collector-date strings ordered by their starting date.
type CollectorsByNameDate []collectorDateString

func (c CollectorsByNameDate) String() string {
	var ret strings.Builder
	for i := range c {
		ret.WriteString(fmt.Sprintf("[name:%s, sTime:%s etime:%s],", c[i].colName, c[i].startDate, c[i].endDate))
	}
	return ret.String()
}

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
	//fmt.Printf("looking for %s , date :%v in slice:%s\n", colname, date, c)
	//find a possible index
	ind := sort.Search(c.Len(), func(i int) bool {
		return (c[i].colName == colname && c[i].startDate.After(date)) || (c[i].colName == colname && c[i].startDate.Equal(date))
	}) - 1 //XXX observe: This was the bug that Will found in the cache cause if you don't subtract 1 it gives you the pos where it would add the next item.
	if ind >= len(c) { //it's not there
		return 0, false
	}
	//validate that the name is the same
	if c[ind].colName != colname {
		return 0, false
	}
	//catch exact same date
	if c[ind].startDate.Equal(date) {
		return ind, true
	}
	//catch the normal case where it is after the startdate, and before the startdate+duration
	if c[ind].startDate.Before(date) && date.Before(c[ind].endDate) {
		return ind, true
	}
	return 0, false
}

//Add will return a new copy of the sorted array with the new collector date added.
//the time should be the truncated time according to the the duration provided in the arguments of
//a GetNodeTableNameDates() call, as well as the end time to be stime+duration
//the caller should try to insert that new name to the nodes table and if succesful,
//change his collectorsByNameDate reference to the new updated one.
//there is a helper func in schemamgr for this called AddNodeAndTableInCache
func (c CollectorsByNameDate) Add(col string, sd time.Time, ed time.Time) (ret CollectorsByNameDate) {
	newnode := NewCollectorDateString(col, sd, ed)
	ret = append(c, *newnode)
	sort.Stable(ret)
	//fmt.Printf("cols from :%+v --> %+v\n", c, ret)
	return
}

// This is a wrapper around sql.Tx, sql.Db, and others we implement
type SqlExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type SqlErrorExecutor interface {
	SqlExecutor
	SetError(error)
}

//handles a strange case where protobuf deserialize an array element of nil as "<nil>"
//and that kills the db insert statement cause it can't make it into a cidr.
func PrefixesToPQArray(n []*net.IPNet) interface {
	driver.Valuer
	sql.Scanner
} {
	if n == nil || len(n) == 0 {
		return nil //database will accept NULL on this field
	}

	ret := make([]string, len(n))
	for ct := range n {
		ret[ct] = n[ct].String()
		if ret[ct] == "" || ret[ct] == "<nil>" {
			//lol someone (protobuf!?) makes this string be <nil>. change it
			//to a database default value
			ret[ct] = "0.0.0.0/0"
		}
	}
	return pq.Array(ret)
}
