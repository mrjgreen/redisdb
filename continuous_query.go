package main

import (
	"time"
	"fmt"
	log "gopkg.in/inconshreveable/log15.v2"
	redis "gopkg.in/redis.v3"
)

type ContinuousQueryManager struct{
	Conn *redis.Client
	Prefix string
	Store SeriesStore
	Log log.Logger
	ComputeInterval string
}

type ContinuousQuery struct{
	TargetSeries string
	Granularity string
	Query SeriesSearch
}

func (self *ContinuousQueryManager) Add(){

}

func (self *ContinuousQueryManager) Delete(){

}

func (self *ContinuousQueryManager) Apply(query ContinuousQuery){

	// Calculate last two time periods based on granularity

	self.Log.Info(fmt.Sprintf("Applying continuouse query '%s' with granularity '%s'", query.TargetSeries, query.Granularity))

	now := time.Now()

	var interval,_ = time.ParseDuration(query.Granularity);

	// Calculate and set the time range for the query.
	startTime := now.Round(interval)

	if startTime.UnixNano() > now.UnixNano() {
		startTime = startTime.Add(-interval)
	}

	query.Query.Between.End = float64(startTime.Add(interval).UnixNano()) / 1e9
	query.Query.Between.Start = float64(startTime.UnixNano()) / 1e9

	// Perform search and group by
	results := self.Store.Search(query.Query)

	self.Store.Delete(SeriesSearch{
		Name : query.TargetSeries,
		Between : SearchTimeRange{
			Start : query.Query.Between.Start,
			End : query.Query.Between.Start,
		},
	})

	for _, point := range *results{

		self.Store.AddDataPoint(query.TargetSeries, &DataPoint{
			Value : point.Value,
			Time : query.Query.Between.Start,
		})
	}
}

func (self *ContinuousQueryManager) List() []ContinuousQuery {

	items := self.Conn.HGetAllMap(self.Prefix + "config:continuous_query")

	var queries = make([]ContinuousQuery, 0)

	for _, _ = range items.Val(){

		query := ContinuousQuery{

		}

		queries = append(queries, query)
	}

	return queries
}

func (self *ContinuousQueryManager) Start(){

	var duration,_ = time.ParseDuration(self.ComputeInterval);

	go func(){
		for {
			// Read continuous query configurations
			self.Log.Info("Checking continuous queries after " + self.ComputeInterval)

			self.Apply(ContinuousQuery{
				Granularity : "1m",
				TargetSeries : "events_10m",
				Query : SeriesSearch{
					Name: "events",
					Values: SearchValues{
						"campaign" : SearchValue{Column:"campaign"},
						"event" : SearchValue{Column:"event"},
						"count" : SearchValue{Type:"COUNT"},
						"value" : SearchValue{Type:"SUM", Column:"value"},
					},
					Group : SearchGroupBy{
						Enabled : true,
					},
				},
			})

			time.Sleep(duration)
		}
	}()
}
