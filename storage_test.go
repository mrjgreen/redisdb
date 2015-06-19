package main

import (
	"testing"
	"strconv"
	"time"
	//"encoding/json"
	//"fmt"
	"github.com/stretchr/testify/assert"
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

	startTime := float64(time.Now().Unix() - 1)

	for i := 0; i < 10000; i++{

		var campaignTag string

		if i % 4 == 0 {
			campaignTag = "123"
		}else if i % 7 == 0 {
			campaignTag = "456"
		}else {
			campaignTag = "789"
		}

		point := NewDataPoint(DataValue{
			"value" : strconv.Itoa(i),
			"event" : strconv.Itoa(i % 5),
			"campaign" : campaignTag,
		})

		store.AddDataPoint(stream, point)
	}

	endTime := float64(time.Now().Unix() + 1)

	search := SeriesSearch{
		Between: SearchTimeRange{
			Start : startTime,
			End : endTime,
		},
		Values: SearchValues{
			"campaign" : SearchValue{Column:"campaign"},
			"event" : SearchValue{Column:"event"},
			"count" : SearchValue{Type:"COUNT"},
			"value" : SearchValue{Type:"SUM", Column:"value"},
		},
		Group: SearchGroupBy{
			Enabled : true,
			Values: []string{"campaign", "event"},
		},
	}

	results := store.Search(stream, search)


	assert.Equal(t, 15, len(*results), "Campaign 123 result count")

	store.Delete(stream, search)

	results = store.Search(stream, search)

	assert.Equal(t, 0, len(*results), "Campaign 123 result count should now be empty")

	store.DeleteSeries(stream)
}
