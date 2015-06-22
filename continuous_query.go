package main

import (
	"time"
	"fmt"
	log "gopkg.in/inconshreveable/log15.v2"
	redis "gopkg.in/redis.v3"
	"encoding/json"
	"./glob"
	"strings"
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
	SourceSeries string
	Granularity string
	Query SeriesSearch
}

func (self *ContinuousQuery) GetInterval() (time.Duration, error) {

	return time.ParseDuration(self.Granularity);
}

func (self *ContinuousQueryManager) Add(cq ContinuousQuery){
	cqjson, _ := json.Marshal(cq)

	self.Conn.HSet(self.Prefix + "config:continuous_query", cq.TargetSeries, string(cqjson))
}

func (self *ContinuousQueryManager) Delete(target_series string){
	self.Conn.HDel(self.Prefix + "config:continuous_query", target_series)
}

func (self *ContinuousQueryManager) Apply(query ContinuousQuery){

	items := self.Store.ListSeries(query.SourceSeries)

	for _, series := range items {

		self.applyToSeries(series.Name, query)
	}
}

func replaceNameWithCapturedGlob(series, sourceName, targetName string) string{

	matches := &glob.GlobMatches{}

	glob.Glob(sourceName, series, matches)

	for _, match := range matches.Matches {
		targetName = strings.Replace(targetName, "*", match, 1)
	}

	return targetName
}

func (self *ContinuousQueryManager) applyToSeries(series string, query ContinuousQuery){

	targetSeries := replaceNameWithCapturedGlob(series, query.SourceSeries, query.TargetSeries)

	// Calculate last two time periods based on granularity
	self.Log.Info(fmt.Sprintf("Applying continuous query '%s' on series '%s' to series '%s' with granularity '%s'", query.TargetSeries, series, targetSeries, query.Granularity))

	now := time.Now()

	interval, _ := query.GetInterval()

	// TODO - currently doing a single interval but work on getting this running back to check modified intervals
	for i := 0; i < 1; i++ {

		// Calculate and set the time range for the query.
		startTime := now.Round(interval)

		if startTime.UnixNano() > now.UnixNano() {
			startTime = startTime.Add(-interval)
		}

		query.Query.Between.End = float64(startTime.Add(interval).UnixNano())/1e9
		query.Query.Between.Start = float64(startTime.UnixNano())/1e9

		// Perform search and group by
		results := self.Store.Search(series, query.Query)

		// Todo - apply in transaction
		self.Store.Delete(targetSeries, SeriesSearch{
			Between : SearchTimeRange{
				Start : query.Query.Between.Start,
				End : query.Query.Between.Start,
			},
		})

		for _, point := range *results {

			self.Store.AddDataPoint(targetSeries, &DataPoint{
				Values : point.Values,
				Time : query.Query.Between.Start,
			})
		}

		self.Log.Info(fmt.Sprintf("Written %d rows for continuous query '%s'", len(*results), targetSeries))
	}
}

func (self *ContinuousQueryManager) List() []ContinuousQuery {

	items := self.Conn.HGetAllMap(self.Prefix + "config:continuous_query")

	queries := make([]ContinuousQuery, 0)

	for _, item := range items.Val(){

		query := ContinuousQuery{}

		json.Unmarshal([]byte(item), &query)

		queries = append(queries, query)
	}

	return queries
}

func (self *ContinuousQueryManager) Start(){

	var duration,_ = time.ParseDuration(self.ComputeInterval);

	for {
		self.Log.Info("Checking continuous queries after " + self.ComputeInterval)

		queries := self.List()

		for _, query := range queries {
			self.Apply(query)
		}

		time.Sleep(duration)
	}
}
