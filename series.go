package main

import (
	"time"
	"./reduce"
	"encoding/json"
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
	Time float64
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
	Start float64
	End float64
}

// A list of data columns to group by
type SearchGroupBy struct {
	Enabled bool
	Columns []string
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

type ResultSet interface {
	Add(SeriesData)
	Get() *Results
}

type resultBucket struct{
	items map[string]*reduce.ReduceFuncIterator
}

type UngroupedResultSet struct {
	results Results
}

func (self *UngroupedResultSet) Add(data SeriesData){
	self.results = append(self.results, data)
}

func (self *UngroupedResultSet) Get() *Results{
	return &self.results
}


// A special case of group by where there is only one bucket
type GroupAllResultSet struct {
	Values SearchValues
	bucket resultBucket
	time float64
}

func getColumnEvaluator(col_type string) *reduce.ReduceFuncIterator{
	var handler reduce.ReduceFunc

	switch col_type{
	case "COUNT" :
		handler = reduce.ReduceCount{}
	case "SUM" :
		handler = reduce.ReduceSum{}
	case "AVG" :
		handler = reduce.ReduceMeanAvg{}
	default :
		handler = reduce.ReduceLastItem{}
	}

	return reduce.NewReduceHandler(handler)
}

func (self *GroupAllResultSet) Add(record SeriesData){

	self.time = record.Time

	for target, source := range self.Values {

		if _,ok := self.bucket.items[target]; !ok {

			self.bucket.items[target] = getColumnEvaluator(source.Type)
		}

		self.bucket.items[target].ReduceNext(record.Values[source.Column])
	}
}

func (self *GroupAllResultSet) Get() *Results{

	var values = make(DataValue)

	for name, item := range self.bucket.items{
		values[name] = item.Result()
	}

	return &Results{SeriesData{Values: values, Time: self.time}}
}

// A group by with a result bucket for each group by
type GroupByColumnResultSet struct {
	Values SearchValues
	cols []string
	buckets map[string]resultBucket
	time float64
}

func encodeStruct(group []string) string{
	groupbyte, _ := json.Marshal(group)

	return string(groupbyte)
}

func (self *GroupByColumnResultSet) Add(record SeriesData){
	// TODO - move this into bucket
	self.time = record.Time

	var group = make([]string, 0)

	for _, col := range self.cols {
		group = append(group, col, record.Values[col].(string))
	}

	groupstr := encodeStruct(group)

	if _, ok := self.buckets[groupstr]; !ok {
		self.buckets[groupstr] = NewResultBucket()
	}

	for target, source := range self.Values {

		if _,ok := self.buckets[groupstr].items[target]; !ok {

			self.buckets[groupstr].items[target] = getColumnEvaluator(source.Type)
		}

		self.buckets[groupstr].items[target].ReduceNext(record.Values[source.Column])
	}
}

func (self *GroupByColumnResultSet) Get() *Results{

	var results = make(Results, len(self.buckets))

	var i = 0

	for _, bucket := range self.buckets{
		var values = make(DataValue)

		for name, item := range bucket.items{
			values[name] = item.Result()
		}

		results[i] = SeriesData{Values: values, Time: self.time}

		i++
	}

	return &results
}

func NewResultBucket() resultBucket{
	return resultBucket{
		items: make(map[string]*reduce.ReduceFuncIterator, 0),
	}
}

func NewResultBucketSet() map[string]resultBucket{
	return make(map[string]resultBucket)
}

func getResultSetHandler(search SeriesSearch) ResultSet{

	if !search.Group.Enabled{
		return &UngroupedResultSet{}
	}

	if search.Group.Columns == nil{
		return &GroupAllResultSet{Values:search.Values, bucket: NewResultBucket()}
	}

	return &GroupByColumnResultSet{cols:search.Group.Columns, Values:search.Values, buckets: NewResultBucketSet()}
}

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
func NewRangeBefore(age_seconds float64) SearchTimeRange{
	return SearchTimeRange{
		End : timeNow() - age_seconds,
	}
}

// Create a SearchTimeRange which will return all records newer than (and including)
// the given age in seconds with decimal fractions
func NewRangeAfter(age_seconds float64) SearchTimeRange{
	return SearchTimeRange{
		Start : timeNow() - age_seconds,
	}
}

// A helper function to create the current timestamp in seconds with decimal nano seconds
func timeNow() float64{
	return float64(time.Now().UnixNano()) / float64(time.Second)
}
