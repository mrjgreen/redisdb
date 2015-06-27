package main

import (
	"fmt"
	"time"

	"github.com/mrjgreen/redisdb/glob"
	"github.com/mrjgreen/redisdb/utils"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type MongoSeriesStore struct {
	Conn *mgo.Database
	Log  utils.Logger
}

func createPipeline(search SeriesSearch) []bson.M {

	// Build the group by statement
	group := bson.M{
		"_id": search.Group.Columns,

		// We always return time - choose the minimum from the group
		"time": bson.M{"$min": "$time"},
	}

	// Project the group by _id onto standard return values - again time is always projected
	project := bson.M{
		"_id":  0,
		"time": "$time",
	}

	// Add the columns to aggregate, and project them
	for k, v := range search.Values {
		group[k] = v
		project[k] = "$" + k
	}

	// Add in the id fields
	for k, _ := range search.Group.Columns {
		project[k] = "$_id." + k
	}

	pipeline := []bson.M{
		{
			"$match": bson.M{
				"time": bson.M{
					"$gte": search.Between.Start,
					"$lte": search.Between.End,
				},
			},
		},
		{
			"$group": group,
		},
		{
			"$project": project,
		},
	}

	fmt.Printf("%v", pipeline)

	return pipeline
}

func (self *MongoSeriesStore) Search(series string, search SeriesSearch) *Results {

	var results Results

	var err error

	if search.Group.Enabled {

		pipeline := createPipeline(search)

		err = self.Conn.C(series).Pipe(pipeline).All(&results)
	} else {

		err = self.Conn.C(series).Find(bson.M{
			"time": bson.M{
				"$gte": search.Between.Start,
				"$lte": search.Between.End,
			},
		}).All(&results)
	}

	if err != nil {
		panic(err)
	}

	return &results
}

func (self *MongoSeriesStore) Delete(series string, between SearchTimeRange) {

	self.Log.Debugf("Deleting series: %s between %s and %s", series, between.Start, between.End)

	self.Conn.C(series).RemoveAll(bson.M{
		"time": bson.M{
			"$gte": between.Start,
			"$lte": between.End,
		},
	})
}

func (self *MongoSeriesStore) Insert(series string, data DataValue) error {

	if len(data) == 0 {
		return fmt.Errorf("Attempted to insert empty value set into series: %s", series)
	}

	// TODO - this is messy!
	var mdata = make(map[string]interface{})

	for k, v := range data {
		mdata[k] = v
	}

	if _, ok := mdata["time"]; !ok {
		mdata["time"] = time.Now()
	}

	err := self.Conn.C(series).Insert(mdata)

	if err != nil {
		panic(err)
	}

	return nil
}

func (self *MongoSeriesStore) List(filter string) []Series {

	val, _ := self.Conn.CollectionNames()

	var results = make([]Series, 0)

	for _, z := range val {

		var matches glob.GlobMatches

		if filter != "" && glob.Glob(filter, z, &matches) {
			results = append(results, Series{Name: z, Matches: matches.Matches})
		}
	}

	return results

	return nil
}

func (self *MongoSeriesStore) Drop(series string) error {

	self.Log.Infof("Deleting series: %s", series)

	self.Conn.C(series).DropCollection()

	return nil
}
