package main

import (
	"time"
)

// Data values are a string key representing the item name
// along with a the value which should be scalar int or float, string
// it is possible to store complex types, which can be serialized as JSON.
// Aggregate functions like SUM/AVG will only work on numeric types
type DataValue map[string]interface {}

// This type is passed into the storage interface when inserting data. The Time field
// should be a unix timestamp and may use decimals to represent fractions of seconds
type SeriesData struct{
	Values DataValue
	Time time.Time
}



///////////////////////////
//
// Querying
//
///////////////////////////

// When searching/deleting data a range must be provided
// Optionally leave Start/End uninitialized (0) to perform a search across the complete range
// The search is inclusive
type SearchTimeRange struct {
	Start time.Time
	End time.Time
}

type GroupColumn map[string]interface{}

// A list of data columns to group by
type SearchGroupBy struct {
	Enabled bool
	Columns GroupColumn
}

// A list of the result column criteria
type SearchValue struct {
	Type string
	Column string
}

type SearchValues map[string]SearchValue

type SeriesSearch struct{
	Values SearchValues
	Between SearchTimeRange
	Group SearchGroupBy
}




///////////////////////////
//
// Result Processing
//
///////////////////////////

type Results []SeriesData


///////////////////////////
//
// General series stuff
//
///////////////////////////
type Series struct {
	Name string
}

type SeriesStore interface{
	Insert(series string, data *SeriesData) error
	Delete(series string, data SearchTimeRange)
	Search(series string, data SeriesSearch) *Results
	List(filter string) []Series
	//Info(series string) Series
	Drop(series string) error
}

///////////////////////////
//
// Helper Functions
//
///////////////////////////

// Create a new series data item using the current time as the timestamp
func NewSeriesData(values DataValue) *SeriesData{
	return &SeriesData{
		Values : values,
		Time : timeNow(),
	}
}

// Create a SearchTimeRange which will return all records older than (and including)
// the given age in seconds with decimal fractions
func NewRangeBefore(age_seconds time.Duration) SearchTimeRange{
	return SearchTimeRange{
		End : timeNow().Add(-age_seconds),
	}
}

// Create a SearchTimeRange which will return all records newer than (and including)
// the given age in seconds with decimal fractions
func NewRangeAfter(age_seconds time.Duration) SearchTimeRange{
	return SearchTimeRange{
		Start : timeNow().Add(-age_seconds),
	}
}

// A helper function to create the current timestamp in seconds with decimal nano seconds
func timeNow() time.Time{
	return time.Now()
}
