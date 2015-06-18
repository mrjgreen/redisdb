package main

import (
	"os"
	//"time"
	"strconv"
	log "gopkg.in/inconshreveable/log15.v2"
	redis "gopkg.in/redis.v3"
)

// Server represents a container for the metadata and storage data and services.
// It is built using a Config and it manages the startup and shutdown of all
// services in the proper order.
type Server struct {
	Log log.Logger
	Store SeriesStore
	Http *HttpInterface
	RetentionPolicyManager *RetentionPolicyManager
	ContinuousQueryManager *ContinuousQueryManager
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

	client := redis.NewClient(&redis.Options{
		Addr:     c.Redis.Host,
		Password: c.Redis.Auth,
		DB:       0,
	})

	log := newLogger(c.Log)

	store := &RedisSeriesStore{
		Conn : client,
		Prefix : c.Redis.KeyPrefix,
		Log: log,
	}

	retention := &RetentionPolicyManager{
		Conn : client,
		Store : store,
		Prefix : c.Redis.KeyPrefix,
		CheckInterval : c.Retention.CheckInterval,
		Log: log,
	}

	cq := &ContinuousQueryManager{
		Conn : client,
		Store : store,
		Prefix : c.Redis.KeyPrefix,
		ComputeInterval : c.ContinuousQuery.ComputeInterval,
		Log: log,
	}

	http := &HttpInterface{
		BindAddress : c.HTTP.Port,
		Store : store,
		Log : log,
	}

	s := &Server{
		Store : store,
		Log : log,
		Http : http,
		RetentionPolicyManager : retention,
		ContinuousQueryManager : cq,
	}

	return s, nil
}

func (s *Server) runTestInserts() error {

	s.Log.Info("Running test inserts")

	s.RetentionPolicyManager.Delete("events*")
	s.RetentionPolicyManager.Add(RetentionPolicy{"events", uint64(120)})

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

		point := NewDataPoint(DataValue{
			"value" : strconv.Itoa(i),
			"event" : strconv.Itoa(i % 5),
			"campaign" : campaignTag,
		})

		s.Store.AddDataPoint("events", point)

		//time.Sleep(10 * time.Millisecond)
	}

	return  nil
}

func (s *Server) Start() error {

	s.Http.Start()
	s.RetentionPolicyManager.Start()
	s.ContinuousQueryManager.Start()

	s.Log.Info("Started server")

	go s.runTestInserts()

	return  nil
}

func (s *Server) Stop() error {

	s.Log.Info("Stopped server")

	return  nil
}
