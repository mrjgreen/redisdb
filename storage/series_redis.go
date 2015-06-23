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

type storagePacket struct {
	Id string
	Value DataValue
}

type resultsMap map[string]interface{}

// The results map is a temporary struct used to bucket items when grouping
// This function returns converts the struct to a real Results object containing
// just the values, discarding the bucket names (map keys)
func (m *resultsMap) getResults() *Results{
	vals := make(Results, len(*m))

	i := 0
	for _,v := range *m {
		vals[i] = v
		i++
	}

	return &vals
}

func (self SearchTimeRange) convertToZRange() redis.ZRangeByScore{

	if self.Start == float64(0) {
		self.Start = "-inf"
	}

	if self.End == float64(0) {
		self.End = "inf"
	}

	return redis.ZRangeByScore{
		Min: strconv.FormatFloat(self.Start, 'f', -1, 64),
		Max: strconv.FormatFloat(self.End, 'f', -1, 64),
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

func (self *RedisSeriesStore) Search(series string, data SeriesSearch) *Results{

	items := self.getRawResults(series, data)

	if !data.Group.Enabled {
		return self.searchKeys(data, items)
	}

	return self.searchGroupedKeys(data, items)
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

	self.DeleteSeriesIfEmpty(series)
}


func (self *RedisSeriesStore) searchGroupedKeys(data SeriesSearch, records *[]redis.Z) *Results{

	var results = Results{}

	for _, z := range *records{

		var record storagePacket

		json.Unmarshal([]byte(z.Member), &record)

		point := &DataPoint{
			Values : record.Value,
			Time : z.Score,
			Timestr : strconv.FormatFloat(z.Score, 'f', -1, 64),
		}

		results = append(results, point)
	}

	return results.getResults()
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
