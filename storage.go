package main

import (
	"time"
	"fmt"
	"strconv"
	redis "gopkg.in/redis.v3"
	"github.com/mrjgreen/simpleflake"
)

type DataValue map[string]string

type DataTags map[string]string

type DataPoint struct{
	Name string
	Id string
	Value DataValue
	Tags DataTags
	Time float64
}

type SeriesStore interface{
	AddDataPoint(*DataPoint) error
	DeleteSeries(string) error
}

type RedisSeriesStore struct{
	Conn *redis.Client
	Prefix string
}

func NewDataPoint(name string, values DataValue) *DataPoint{
	return &DataPoint{
		Name : name,
		Value : values,
		Id : strconv.FormatUint(simpleflake.NewId().Id, 10),
		Time : float64(time.Now().Unix()),
	}
}

func (self *RedisSeriesStore) AddDataPoint(data *DataPoint) error{

	self.Conn.Multi().Exec(func() error{
		self.Conn.ZAdd(self.Prefix + "data:" + data.Name, redis.Z{data.Time, data.Id})
		self.Conn.HMSet(self.Prefix + "data:" + data.Name + ":id:" + data.Id, data.Value)

		for k,v := range data.Tags {
			self.Conn.ZAdd(self.Prefix + "data:" + data.Name + ":tags:" + k + ":" + v, redis.Z{data.Time, data.Id})
		}
		return nil
	})


	//self.Conn.Exec()

	return nil
}

func (self *RedisSeriesStore) DeleteSeries(series string) error{
	// Scan through keys
	var cursor = 0

	for {
		r := self.Conn.Scan(cursor, "MATCH", self.Prefix + "data:" + series + "*", "COUNT", 100)

		cursor, _ = r.Elems[0].Int()

		items, _ := r.Elems[1].List()

		if len(items) == 0{
			return nil
		}

		for _, key := range items{
			self.Conn.Del(key)
			fmt.Println("Removing key " + key)
		}
	}

	return nil
}
