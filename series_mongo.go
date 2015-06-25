package main

import (
	"fmt"
	"time"
	log "gopkg.in/inconshreveable/log15.v2"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"./glob"
)

type MongoSeriesStore struct{
	Conn *mgo.Database
	Log log.Logger
}

func seriesLog(action string, series string, start time.Time, end time.Time){
	log.Debug(fmt.Sprintf("%s series: %s between %s and %s", action, series, start, end))
}

func createPipeline(search SeriesSearch) []bson.M{

	group := bson.M{
		"_id": search.Group.Columns,
	}

	for k, v := range search.Values{
		group[k] = v
	}

	pipeline := []bson.M{
		{
			"$match" : bson.M{
				"time": bson.M{
					"$gt": search.Between.Start,
					"$lt": search.Between.End,
				},
			},
		},
		{
			"$group" : group,
		},
	}

	fmt.Printf("%v", pipeline)

	return pipeline
}

func (self *MongoSeriesStore) Search(series string, search SeriesSearch) *Results{

	var results Results

	var err error

	if(search.Group.Enabled){

		pipeline := createPipeline(search)

		err = self.Conn.C(series).Pipe(pipeline).All(&results)
	}else{

		err = self.Conn.C(series).Find(bson.M{
			"time": bson.M{
				"$gt": search.Between.Start,
				"$lt": search.Between.End,
			},
		}).All(&results)
	}

	if err != nil{
		panic(err)
	}

	return &results
}

func (self *MongoSeriesStore) Delete(series string, between SearchTimeRange){

	seriesLog("Deleting", series, between.Start, between.End)

	self.Conn.C(series).RemoveAll(bson.M{
		"time": bson.M{
			"$gt": between.Start,
			"$lt": between.End,
		},
	})
}


func (self *MongoSeriesStore) Insert(series string, data *SeriesData) error{

	if data.Values == nil{
		return fmt.Errorf("Attempted to insert empty value set into series: " + series)
	}

	data.Time = timeNow()

	err := self.Conn.C(series).Insert(data)

	if err != nil{
		panic(err)
	}

	return nil
}

func (self *MongoSeriesStore) List(filter string) []Series{

	val, _ := self.Conn.CollectionNames()

	var results = make([]Series, 0)

	for _, z := range val{

		var matches glob.GlobMatches

		if(filter != "" && glob.Glob(filter, z, &matches)){
			results = append(results, Series{Name: z})
		}
	}

	return results

	return nil
}

func (self *MongoSeriesStore) Drop(series string) error{

	log.Info("Deleting series: " + series)

	self.Conn.C(series).DropCollection()

	return nil
}
