package main

import (
	"errors"
	"strings"
	"time"

	"github.com/mrjgreen/redisdb/utils"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type ContinuousQueryManager struct {
	Conn            *mgo.Database
	Store           SeriesStore
	Log             utils.Logger
	ComputeInterval string
	stop            chan struct{}
}

type ContinuousQuery struct {
	TargetSeries string
	SourceSeries string
	Granularity  string
	Query        SeriesSearch
}

func (self *ContinuousQuery) GetInterval() (time.Duration, error) {

	return time.ParseDuration(self.Granularity)
}

func (self *ContinuousQueryManager) Add(cq ContinuousQuery) {
	self.Conn.C("continuous_query").Insert(cq)

	// Index
	index := mgo.Index{
		Key:        []string{"targetseries"},
		Unique:     true,
		DropDups:   true,
		Background: true,
		Sparse:     true,
	}

	self.Conn.C("continuous_query").EnsureIndex(index)
}

func (self *ContinuousQueryManager) Delete(target_series string) {
	self.Conn.C("continuous_query").RemoveAll(bson.M{
		"targetseries": target_series,
	})
}

func (self *ContinuousQueryManager) Apply(query ContinuousQuery) {

	items := self.Store.List(query.SourceSeries)

	self.Log.Infof("Found %d series to apply continuous query %s", len(items), query.SourceSeries)

	for _, series := range items {

		self.applyToSeries(series, query)
	}
}

func replaceNameWithCapturedGlob(matches []string, targetName string) string {

	for _, match := range matches {
		targetName = strings.Replace(targetName, "*", match, 1)
	}

	return targetName
}

func (self *ContinuousQueryManager) applyToSeries(seriesstr Series, query ContinuousQuery) {

	series := seriesstr.Name

	targetSeries := replaceNameWithCapturedGlob(seriesstr.Matches, query.TargetSeries)

	// Calculate last two time periods based on granularity
	self.Log.Infof("Applying continuous query '%s' on series '%s' to series '%s' with granularity '%s'", query.TargetSeries, series, targetSeries, query.Granularity)

	now := time.Now()

	interval, _ := query.GetInterval()

	// TODO - currently doing a single interval but work on getting this running back to check modified intervals
	for i := 0; i < 1; i++ {

		// Calculate and set the time range for the query.
		startTime := now.Round(interval)

		if startTime.UnixNano() > now.UnixNano() {
			startTime = startTime.Add(-interval)
		}

		query.Query.Between.End = startTime.Add(interval).Add(-time.Nanosecond)
		query.Query.Between.Start = startTime

		// Perform search and group by
		results := self.Store.Search(series, query.Query)

		// Todo - apply in transaction
		self.Store.Delete(targetSeries, SearchTimeRange{
			Start: query.Query.Between.Start,
			End:   query.Query.Between.End,
		})

		for _, point := range *results {

			point["time"] = query.Query.Between.Start

			self.Store.Insert(targetSeries, point)
		}

		self.Log.Infof("Written %d rows for continuous query '%s'", len(*results), targetSeries)
	}
}

func (self *ContinuousQueryManager) List() []ContinuousQuery {

	var queries []ContinuousQuery

	self.Conn.C("continuous_query").Find(nil).All(&queries)

	return queries
}

func (self *ContinuousQueryManager) Start() error {

	var duration, err = time.ParseDuration(self.ComputeInterval)

	if err != nil {
		return err
	}

	if self.stop != nil {
		return errors.New("Continuous query manager is already running")
	}

	self.stop = make(chan struct{})

	self.Log.Infof("Started continuous query manager running every %s", duration)

	for {
		select {
		case <-self.stop:
			return nil
		case <-time.After(duration):
			queries := self.List()

			self.Log.Infof("Checking %d continuous queries after %s", len(queries), self.ComputeInterval)

			for _, query := range queries {
				self.Apply(query)
			}
		}
	}
}

// Close closes the underlying listener.
func (self *ContinuousQueryManager) Stop() {
	if self.stop == nil {
		return
	}

	close(self.stop)
	self.stop = nil

	self.Log.Infof("Stopped continuous query manager")
}
