package main

import (
	"testing"
	log "gopkg.in/inconshreveable/log15.v2"
	redis "gopkg.in/redis.v3"
)

func TestRedis(t *testing.T)  {

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		DB:       0,
	})

	store := &RedisSeriesStore{Conn : client, Prefix : "testpref"}

	store.Log = log.New()

	stream := "test"

	point := NewDataPoint(stream, DataValue{"something" : "1", "else" : "2"})

	point.Tags = DataTags{"campaign" : "1234"}

	store.AddDataPoint(point)

	search := SeriesSearch{
		Name: stream,
		//Values: SearchValues{},
		Tags: SearchTags{
			"campaign" : []string{"1234"},
		},
		Between: SearchTimeRange{
			Start : point.Time - 10.0,
			End : point.Time + 10.0,
		},
		//Group: SearchGroupBy{},
	}

	store.Search(search)

	store.DeleteSeries(stream)
}
