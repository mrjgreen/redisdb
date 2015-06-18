package main

import (
	"time"
	"fmt"
	"strconv"
	"encoding/json"
	log "gopkg.in/inconshreveable/log15.v2"
	redis "gopkg.in/redis.v3"
	"./simpleflake"
	"github.com/ryanuber/go-glob"
)

type DataValue map[string]interface{}

type DataTags map[string]string

type DataPoint struct{
	Name string
	Id string
	Value DataValue
	Time float64
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
	Name string
	Values SearchValues
	Between SearchTimeRange
	Group SearchGroupBy
}

func NewSearchOlderThan(name string, age_seconds uint64) SeriesSearch{
	return SeriesSearch{
		Name : name,
		Between : SearchTimeRange{
			End : float64(uint64(time.Now().Unix()) - age_seconds),
		},
	}
}

type SeriesStore interface{
	AddDataPoint(*DataPoint) error
	DeleteSeries(string) error
	Search(data SeriesSearch) *Results
	Delete(data SeriesSearch)
	ListSeries(filter string) []Series
}

type RedisSeriesStore struct{
	Conn *redis.Client
	Prefix string
	Log log.Logger
}

type Results []*ResultPoint
type ResultsMap map[string]*ResultPoint

type ResultPoint struct{
	Id string
	Value DataValue
	Time float64
}

type Series struct {
	Name string
	Created float64
}

func expandMapToArray(m map[string]string) []string{

	arr := make([]string,0)

	for k,v := range m {
		arr = append(arr, k, v)
	}

	return arr
}

func NewDataPoint(name string, values DataValue) *DataPoint{
	return &DataPoint{
		Name : name,
		Value : values,
		Id : simpleflake.NewId().String(),
		Time : float64(time.Now().Unix()),
	}
}

func getDataBetweenScore(data SeriesSearch) redis.ZRangeByScore{

	var end, start string

	if data.Between.End == float64(0) {
		end = "inf"
	}else{
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

func (self *RedisSeriesStore) getRawResults(data SeriesSearch) *[]redis.Z{

	// Get the start and end times for the search
	score := getDataBetweenScore(data)

	zvalkey := self.Prefix + "data:" + data.Name

	message := fmt.Sprintf("Searching series: %s between range %s and %s on key %s", data.Name, score.Min, score.Max, zvalkey)

	log.Debug(message)

	result := self.Conn.ZRangeByScoreWithScores(zvalkey, score)

	items, err := result.Result()

	if err != nil {
		panic(err)
	}

	return &items
}

func (self *RedisSeriesStore) Search(data SeriesSearch) *Results{

	items := self.getRawResults(data)

	if !data.Group.Enabled {
		return self.searchKeys(data, items)
	}

	return self.searchGroupedKeys(data, items)
}

func (self *RedisSeriesStore) Delete(data SeriesSearch){

	// Get the start and end times for the search
	score := getDataBetweenScore(data)

	zvalkey := self.Prefix + "data:" + data.Name

	message := fmt.Sprintf("Deleting from series: %s between range %s and %s on key %s", data.Name, score.Min, score.Max, zvalkey)

	log.Debug(message)

	result := self.Conn.ZRemRangeByScore(zvalkey, score.Min, score.Max)

	items, err := result.Result()

	if err != nil {
		panic(err)
	}

	message = fmt.Sprintf("Deleted %d items from series: %s between range %s and %s on key %s", items, data.Name, score.Min, score.Max, zvalkey)

	log.Debug(message)

	self.DeleteSeriesIfEmpty(data.Name)
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

		var record StorageDataPacket

		json.Unmarshal([]byte(z.Member), &record)

		for _, col := range data.Group.Values {
			group = append(group, col, record.Value[col].(string))
		}

		groupbyte, _ := json.Marshal(group)

		groupstr := string(groupbyte)

		if results[groupstr] == nil {

			results[groupstr] = &ResultPoint{
				Value : DataValue{},
				Time : z.Score,
				Id : simpleflake.NewId().String(),
			}
		}

		for target, source := range data.Values {

			if source.Type == "COUNT" {

				if results[groupstr].Value[target] == nil {
					results[groupstr].Value[target] = 0
				}

				results[groupstr].Value[target] = results[groupstr].Value[target].(int) + 1

			}else if source.Type == "SUM" {

				if results[groupstr].Value[target] == nil {
					results[groupstr].Value[target] = 0.0
				}

				flt, _ := strconv.ParseFloat(record.Value[source.Column].(string), 64)

				results[groupstr].Value[target] = results[groupstr].Value[target].(float64) + flt
			}else {
				results[groupstr].Value[target] = record.Value[source.Column].(string)
			}
		}
	}

	return resultMapToResultArray(&results)
}

func (self *RedisSeriesStore) searchKeys(data SeriesSearch, ids *[]redis.Z) *Results{

	var results = Results{}

	for _, z := range *ids{

		var record StorageDataPacket

		json.Unmarshal([]byte(z.Member), &record)

//		values := DataValue{}
//
//		for target, source := range data.Values{
//			values[target] = record.Value[source.Column]
//		}

		point := &ResultPoint{
			Value : record.Value,
			Time : z.Score,
			Id : record.Id,
		}

		results = append(results, point)
	}

	return &results
}

type StorageDataPacket struct {
	Id string
	Value DataValue
}

func (self *RedisSeriesStore) AddDataPoint(data *DataPoint) error{

	if data.Value == nil{
		return fmt.Errorf("Attempted to insert empty value set into series: " + data.Name)
	}

	val_str, _ := json.Marshal(StorageDataPacket{Id : data.Id, Value : data.Value})

	z_val := redis.Z{Score: data.Time, Member: string(val_str)}


	self.Conn.SAdd(self.Prefix + "meta:series", data.Name)

	self.Conn.ZAdd(self.Prefix + "data:" + data.Name, z_val)

	return nil
}

func (self *RedisSeriesStore) ListSeries(filter string) []Series{

	val, _ := self.Conn.SMembers(self.Prefix + "meta:series").Result()

	var results = make([]Series, 0)

	for _, z := range val{

		if(filter != "" && glob.Glob(filter, z)){
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
