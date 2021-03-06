package main

import (
	"os"
	"fmt"
	"encoding/json"
	log "gopkg.in/inconshreveable/log15.v2"
	redis "gopkg.in/redis.v3"
)

// Server represents a container for the metadata and storage data and services.
// It is built using a Config and it manages the startup and shutdown of all
// services in the proper order.
type Server struct {
	Log log.Logger
	Store SeriesStore
}

func newRedisStore(c *RedisConfig) (*RedisSeriesStore, error){

	client := redis.NewClient(&redis.Options{
		Addr:     c.Host,
		Password: c.Auth,
		DB:       0,
	})

	return &RedisSeriesStore{Conn : client, Prefix : c.KeyPrefix}, nil
}

func newLogger(c *LogConfig) log.Logger{

	if !c.Enabled{
		return nil
	}

	var l = log.New()

	level, err := log.LvlFromString(c.Level)

	if err != nil{
		panic("Level could not be read")
	}

	l.SetHandler(log.LvlFilterHandler(
		level,
		log.StreamHandler(os.Stderr, log.TerminalFormat()),
	))

	return l
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c *Config) (*Server, error) {

	store, err := newRedisStore(c.Redis)

	if err != nil{
		return nil, err
	}

	s := &Server{
		Store : store,
		Log : newLogger(c.Log),
	}

	store.Log = s.Log

	point := NewDataPoint("test", DataValue{"something" : "1", "else" : "2"})
	point.Tags = DataTags{"campaign" : "1234"}

	s.Store.AddDataPoint(point)

	search := SeriesSearch{
		Name: "test",
		Tags: SearchTags{
			"campaign" : []string{"1234"},
		},
		Between: SearchTimeRange{
			Start : point.Time - 10.0,
			End : point.Time + 10.0,
		},
		Values: SearchValues{"campaign" : SearchValue{
			Column : "campaign",
		}},
		//Group: SearchGroupBy{},
	}

	results := store.Search(search)

	res, _ := json.Marshal(results)

	fmt.Printf("%s", res)

	//s.Store.DeleteSeries("test")

	return s, nil
}


func (s *Server) Start() error {

	s.Log.Info("Started server")

	return  nil
}

func (s *Server) Stop() error {

	s.Log.Info("Stopped server")

	return  nil
}
