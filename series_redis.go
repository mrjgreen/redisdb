package main

import (
	"fmt"
	"strconv"
	"encoding/json"
	log "gopkg.in/inconshreveable/log15.v2"
	redis "gopkg.in/redis.v3"
	"./simpleflake"
	"./glob"
)

type RedisSeriesStore struct{
	Conn *redis.Client
	Prefix string
	Log log.Logger
}

type storagePacket struct {
	Id string
	Values DataValue
}

func NewStoragePacket(data DataValue) storagePacket{
	return storagePacket{
		Id : simpleflake.NewId().String(),
		Values: data,
	}
}

func NewStoragePacketFromString(data string) storagePacket{

	var record storagePacket

	json.Unmarshal([]byte(data), &record)

	return record
}

func (self storagePacket) Serialize() string{

	val_str, err := json.Marshal(self)

	if err != nil{
		panic(err)
	}

	return string(val_str)
}

func (self storagePacket) GetSeriesData(time float64) SeriesData{
	return SeriesData{
		Values: self.Values,
		Time: time,
	}
}


func (self SearchTimeRange) convertToZRange() redis.ZRangeByScore{

	var start, end string

	if self.Start == float64(0) {
		start = "-inf"
	}else{
		start = strconv.FormatFloat(self.Start, 'f', -1, 64)
	}

	if self.End == float64(0) {
		end = "inf"
	}else{
		end = strconv.FormatFloat(self.End, 'f', -1, 64)
	}

	return redis.ZRangeByScore{
		Min: start,
		Max: end,
	}
}

func seriesLog(action, series, key, start, end string){
	log.Debug(fmt.Sprintf("%s series: %s on key %s between %s and %s", action, series, key, start, end))
}

func (self *RedisSeriesStore) getRawResults(series string, between SearchTimeRange) *[]redis.Z{

	score := between.convertToZRange()

	zvalkey := self.Prefix + "data:" + series

	seriesLog("Searching", series, zvalkey, score.Min, score.Max)

	result := self.Conn.ZRangeByScoreWithScores(zvalkey, score)

	items, err := result.Result()

	if err != nil {
		panic(err)
	}

	return &items
}

func (self *RedisSeriesStore) Search(series string, search SeriesSearch) *Results{

	records := self.getRawResults(series, search.Between)

	results := getResultSetHandler(search)

	for _, z := range *records{

		record := NewStoragePacketFromString(z.Member)

		results.Add(record.GetSeriesData(z.Score))
	}

	return results.Get()
}

func (self *RedisSeriesStore) Delete(series string, between SearchTimeRange){

	// Get the start and end times for the search
	score := between.convertToZRange()

	zvalkey := self.Prefix + "data:" + series

	seriesLog("Deleting", series, zvalkey, score.Min, score.Max)

	result := self.Conn.ZRemRangeByScore(zvalkey, score.Min, score.Max)

	items, err := result.Result()

	if err != nil {
		panic(err)
	}

	seriesLog(fmt.Sprintf("Deleted %d items from", items), series, zvalkey, score.Min, score.Max)

	self.deleteSeriesIfEmpty(series)
}


func (self *RedisSeriesStore) Insert(series string, data *SeriesData) error{

	if data.Values == nil{
		return fmt.Errorf("Attempted to insert empty value set into series: " + series)
	}

	if data.Time == 0.0 {
		data.Time = timeNow()
	}

	storage := NewStoragePacket(data.Values)

	z_val := redis.Z{Score: data.Time, Member: storage.Serialize()}

	self.Conn.ZAdd(self.Prefix + "data:" + series, z_val)

	self.Conn.SAdd(self.Prefix + "meta:series", series)

	return nil
}

func (self *RedisSeriesStore) List(filter string) []Series{

	val, _ := self.Conn.SMembers(self.Prefix + "meta:series").Result()

	var results = make([]Series, 0)

	for _, z := range val{

		var matches glob.GlobMatches

		if(filter != "" && glob.Glob(filter, z, &matches)){
			results = append(results, Series{Name: z})
		}
	}

	return results
}

func (self *RedisSeriesStore) deleteSeriesIfEmpty(series string) error{

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

func (self *RedisSeriesStore) Drop(series string) error{

	log.Info("Deleting series: " + series)

	self.Conn.SRem(self.Prefix + "meta:series", series)

	self.Conn.Del(self.Prefix + "data:" + series)

	return nil
}
