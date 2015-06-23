package main

import (
	log "gopkg.in/inconshreveable/log15.v2"
	"strconv"
	"time"
)

// Server represents a container for the metadata and storage data and services.
// It is built using a Config and it manages the startup and shutdown of all
// services in the proper order.
type BenchMark struct {
	Log log.Logger
	Store SeriesStore
	RetentionPolicyManager *RetentionPolicyManager
	ContinuousQueryManager *ContinuousQueryManager
}

func (s *BenchMark) Start() error {

	s.Log.Info("Running test inserts")

//	s.RetentionPolicyManager.Delete("events:raw:c:*")
//	s.RetentionPolicyManager.Add(RetentionPolicy{"events:raw:c:*", uint64(120)})
//
//	s.ContinuousQueryManager.Add(ContinuousQuery{
//		SourceSeries : "events:raw:c:*",
//		TargetSeries : "events:10m:c:*", // The glob pattern of source will be mapped onto the target
//		Granularity : "1m",
//		Query : SeriesSearch{
//			Values: SearchValues{
//				//"campaign" : SearchValue{Column:"campaign"},
//				"event" : SearchValue{Column:"event"},
//				"count" : SearchValue{Type:"COUNT"},
//				"value" : SearchValue{Type:"SUM", Column:"value"},
//			},
//			Group : SearchGroupBy{
//				Enabled : true,
//			},
//		},
//	})

	var i = 0

	for {

		i++
		var campaignTag string

		if i % 4 == 0 {
			campaignTag = "123"
		}else if i % 7 == 0 {
			campaignTag = "456"
		}else {
			campaignTag = "789"
		}

		point := NewSeriesData(DataValue{
			"value" : strconv.Itoa(i),
			"event" : strconv.Itoa(i % 5),
		})

		s.Store.Insert("click:raw:c:" + campaignTag, point)

		time.Sleep(1 * time.Millisecond)
	}

	return  nil
}
