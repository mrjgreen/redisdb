package main

import (
	log "gopkg.in/inconshreveable/log15.v2"
	//"strconv"
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

	s.RetentionPolicyManager.Delete("click:raw:c:*")

	s.RetentionPolicyManager.Add(RetentionPolicy{"click:raw:c:*", time.Duration(120 * time.Second)})

	s.ContinuousQueryManager.Add(ContinuousQuery{
		SourceSeries : "click:raw:c:*",
		TargetSeries : "click:1m:c:*", // The glob pattern of source will be mapped onto the target
		Granularity : "1m",
		Query : SeriesSearch{
			Values: SearchValues{
				"count" : SearchValue{Type:"COUNT"},
				"event" : SearchValue{Column : "event"},
			},
			Group : SearchGroupBy{Enabled: true,},
		},
	})

	s.ContinuousQueryManager.Add(ContinuousQuery{
		SourceSeries : "click:raw:c:*",
		TargetSeries : "click:event:10m:c:*", // The glob pattern of source will be mapped onto the target
		Granularity : "10m",
		Query : SeriesSearch{
			Values: SearchValues{
				"count" : SearchValue{Type:"COUNT"},
				"avg_value" : SearchValue{Type:"AVG", Column: "value"},
				"sum_value" : SearchValue{Type:"SUM", Column: "value"},
			},
			Group : SearchGroupBy{Enabled: true, Columns : GroupColumn{"event" : "$event"},},
		},
	})

	var i = 0

	for {

		i++
		campaignTag := "12345"

//		var campaignTag string
//
//		if i % 4 == 0 {
//			campaignTag = "123"
//		}else if i % 7 == 0 {
//			campaignTag = "456"
//		}else {
//			campaignTag = "789"
//		}

		point := NewSeriesData(DataValue{
			"value" : i,
			"event" : i % 5,
		})

		s.Store.Insert("click:raw:c:" + campaignTag, point)

		time.Sleep(100 * time.Nanosecond)
	}

	return  nil
}
