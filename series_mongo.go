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

func checkBetweenRange(time_range SearchTimeRange) SearchTimeRange{

	// TODO - check for nil end time and set it to now
	// TODO - check for nil start time and set it to 1970

	return time_range
}

func (self *MongoSeriesStore) Search(series string, search SeriesSearch) *Results{

	var results Results

	between := checkBetweenRange(search.Between)

	if(search.Group.Enabled){

		group := bson.M{
			"_id": search.Group.Columns,
		}

		for k, v := range search.Group.Columns{
			group[k] = v
		}

		pipeline := []bson.M{
			{
				"$match" : bson.M{
					"time": bson.M{
						"$gt": between.Start,
						"$lt": between.End,
					},
				},
			},
			{
				"$group" : group,
			},
		}

		self.Conn.C(series).Pipe(pipeline).All(&results)
	}else{
		self.Conn.C(series).Find(bson.M{
			"time": bson.M{
				"$gt": between.Start,
				"$lt": between.End,
			},
		}).All(&results)
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

	// TODO - delete if emtpy

	//seriesLog(fmt.Sprintf("Deleted %d items from", items), series, string(between.Start), string(between.End))
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
