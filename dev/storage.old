package main

import (
	"time"
	"fmt"
	"strconv"
	"encoding/json"
	log "gopkg.in/inconshreveable/log15.v2"
	redis "gopkg.in/redis.v3"
	"./simpleflake"
	"./glob"
)

type DataValue map[string]interface{}

type DataTags map[string]string

type DataPoint struct{
	Values DataValue `json:"values"`
	Time float64 `json:"-"`
	Timestr string `json:"time"`
}

type SearchTimeRange struct {
	Start float64
	End float64
}

type SearchGroupBy struct {
	Enabled bool
	Values []string
}

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

type SeriesStore interface{
	AddDataPoint(series string, data *DataPoint) error
	Delete(series string, data SeriesSearch)
	Search(series string, data SeriesSearch) *Results
	ListSeries(filter string) []Series
	DeleteSeries(series string) error
}

type RedisSeriesStore struct{
	Conn *redis.Client
	Prefix string
	Log log.Logger
}

type Results []*DataPoint

type ResultsMap map[string]*DataPoint

type Series struct {
	Name string
	Created float64
}

type storagePacket struct {
	Id string
	Value DataValue
}

func NewSearchOlderThan(age_seconds uint64) SeriesSearch{
	return SeriesSearch{
		Between : SearchTimeRange{
			End : float64(uint64(time.Now().Unix()) - age_seconds),
		},
	}
}

func expandMapToArray(m map[string]string) []string{

	arr := make([]string,0)

	for k,v := range m {
		arr = append(arr, k, v)
	}

	return arr
}

func NewDataPoint(values DataValue) *DataPoint{
	return &DataPoint{
		Values : values,
		Time : float64(time.Now().Unix()),
	}
}

func getDataBetweenScore(data SeriesSearch) redis.ZRangeByScore{

	var end, start string

	if data.Between.End == float64(0) {
		end = "inf"
	}else{
		// Exclusive last item makes everything more convenient
		end = strconv.FormatFloat(data.Between.End, 'f', -1, 64)
	}

	if data.Between.Start == float64(0) {
		start = "-inf"
	}else{
		start = strconv.FormatFloat(data.Between.Start, 'f', -1, 64)
	}

	score := redis.ZRangeByScore{
		Min: start,
		Max: end,
	}

	return score
}

func (self *RedisSeriesStore) getRawResults(series string, data SeriesSearch) *[]redis.Z{

	// Get the start and end times for the search
	score := getDataBetweenScore(data)

	zvalkey := self.Prefix + "data:" + series

	message := fmt.Sprintf("Searching series: %s between range %s and %s on key %s", series, score.Min, score.Max, zvalkey)

	log.Debug(message)

	result := self.Conn.ZRangeByScoreWithScores(zvalkey, score)

	items, err := result.Result()

	if err != nil {
		panic(err)
	}

	return &items
}

func (self *RedisSeriesStore) Search(series string, data SeriesSearch) *Results{

	items := self.getRawResults(series, data)

	if !data.Group.Enabled {
		return self.searchKeys(data, items)
	}

	return self.searchGroupedKeys(data, items)
}

func (self *RedisSeriesStore) Delete(series string, data SeriesSearch){

	// Get the start and end times for the search
	score := getDataBetweenScore(data)

	zvalkey := self.Prefix + "data:" + series

	message := fmt.Sprintf("Deleting from series: %s between range %s and %s on key %s", series, score.Min, score.Max, zvalkey)

	log.Debug(message)

	result := self.Conn.ZRemRangeByScore(zvalkey, score.Min, score.Max)

	items, err := result.Result()

	if err != nil {
		panic(err)
	}

	message = fmt.Sprintf("Deleted %d items from series: %s between range %s and %s on key %s", items, series, score.Min, score.Max, zvalkey)

	log.Debug(message)

	self.DeleteSeriesIfEmpty(series)
}

func resultMapToResultArray(m *ResultsMap) *Results{
	vals := make(Results, len(*m))

	i := 0
	for _,v := range *m {
		vals[i] = v
		i += 1
	}

	return &vals
}

func (self *RedisSeriesStore) searchGroupedKeys(data SeriesSearch, ids *[]redis.Z) *Results{

	var results = make(ResultsMap,0)

	for _, z := range *ids{

		var group = make([]string, 0)

		var record storagePacket

		json.Unmarshal([]byte(z.Member), &record)

		for _, col := range data.Group.Values {
			group = append(group, col, record.Value[col].(string))
		}

		groupbyte, _ := json.Marshal(group)

		groupstr := string(groupbyte)

		if results[groupstr] == nil {

			results[groupstr] = &DataPoint{
				Values : DataValue{},
				Time : z.Score,
			}
		}

		for target, source := range data.Values {

			if source.Type == "COUNT" {

				if results[groupstr].Values[target] == nil {
					results[groupstr].Values[target] = 0
				}

				results[groupstr].Values[target] = results[groupstr].Values[target].(int) + 1

			}else if source.Type == "SUM" {

				if results[groupstr].Values[target] == nil {
					results[groupstr].Values[target] = 0.0
				}

				flt, _ := strconv.ParseFloat(record.Value[source.Column].(string), 64)

				results[groupstr].Values[target] = results[groupstr].Values[target].(float64) + flt
			}else {
				results[groupstr].Values[target] = record.Value[source.Column].(string)
			}
		}
	}

	return resultMapToResultArray(&results)
}

func (self *RedisSeriesStore) searchKeys(data SeriesSearch, ids *[]redis.Z) *Results{

	var results = Results{}

	for _, z := range *ids{

		var record storagePacket

		json.Unmarshal([]byte(z.Member), &record)

//		values := DataValue{}
//
//		for target, source := range data.Values{
//			values[target] = record.Value[source.Column]
//		}

		point := &DataPoint{
			Values : record.Value,
			Time : z.Score,
			Timestr : strconv.FormatFloat(z.Score, 'f', -1, 64),
		}

		results = append(results, point)
	}

	return &results
}

func (self *RedisSeriesStore) AddDataPoint(series string, data *DataPoint) error{

	if data.Values == nil{
		return fmt.Errorf("Attempted to insert empty value set into series: " + series)
	}

	if data.Time == 0.0 {
		data.Time = float64(time.Now().Unix())
	}

	val_str, _ := json.Marshal(storagePacket{Id : simpleflake.NewId().String(), Value : data.Values})

	z_val := redis.Z{Score: data.Time, Member: string(val_str)}


	self.Conn.SAdd(self.Prefix + "meta:series", series)

	self.Conn.ZAdd(self.Prefix + "data:" + series, z_val)

	return nil
}

func (self *RedisSeriesStore) ListSeries(filter string) []Series{

	val, _ := self.Conn.SMembers(self.Prefix + "meta:series").Result()

	var results = make([]Series, 0)

	for _, z := range val{

		var matches glob.GlobMatches

		if(filter != "" && glob.Glob(filter, z, &matches)){
			point := Series{
				Name: z,
			}

			results = append(results, point)
		}
	}

	return results
}

func (self *RedisSeriesStore) DeleteSeriesIfEmpty(series string) error{

	log.Info("Deleting series: " + series)

	key := self.Prefix + "data:" + series

	size := self.Conn.ZCard(key).Val()

	if size == 0 {
		self.Conn.SRem(self.Prefix + "meta:series", series)
	}

	size = self.Conn.ZCard(key).Val()

	if size > 0 {
		self.Conn.SAdd(self.Prefix + "meta:series", series)
	}

	return nil
}

func (self *RedisSeriesStore) DeleteSeries(series string) error{

	log.Info("Deleting series: " + series)

	self.Conn.ZRem(self.Prefix + "meta:series", series)

	self.Conn.Del(self.Prefix + "data:" + series)

	return nil
}
