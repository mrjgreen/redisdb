package main

import (
	"github.com/mrjgreen/redisdb/utils"
	"gopkg.in/mgo.v2"
)

// Server represents a container for the metadata and storage data and services.
// It is built using a Config and it manages the startup and shutdown of all
// services in the proper order.
type Server struct {
	mgo                    *mgo.Session
	Log                    utils.Logger
	Store                  SeriesStore
	Http                   *HttpInterface
	RetentionPolicyManager *RetentionPolicyManager
	ContinuousQueryManager *ContinuousQueryManager
	BenchMark              *BenchMark
}

func NewMongo(c *Config) *mgo.Session {
	mgoSession, err := mgo.Dial(c.Mongo.Host)

	if err != nil {
		panic(err)
	}

	// Optional. Switch the session to a monotonic behavior.
	mgoSession.SetMode(mgo.Monotonic, true)

	return mgoSession
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c *Config) (*Server, error) {

	mgoSession := NewMongo(c)

	log, err := utils.NewLogger(c.Log.Level)

	if err != nil {
		return nil, err
	}

	store := &MongoSeriesStore{
		Conn: mgoSession.DB("data"),
		Log:  log,
	}

	retention := &RetentionPolicyManager{
		Conn:          mgoSession.DB("config"),
		Store:         store,
		CheckInterval: c.Retention.CheckInterval,
		Log:           log,
	}

	cq := &ContinuousQueryManager{
		Conn:            mgoSession.DB("config"),
		Store:           store,
		ComputeInterval: c.ContinuousQuery.ComputeInterval,
		Log:             log,
	}

	http := &HttpInterface{
		BindAddress: c.HTTP.Port,
		Store:       store,
		Log:         log,
	}

	test := &BenchMark{
		Store: store,
		Log:   log,
		RetentionPolicyManager: retention,
		ContinuousQueryManager: cq,
	}

	s := &Server{
		mgo:   mgoSession,
		Store: store,
		Log:   log,
		Http:  http,
		RetentionPolicyManager: retention,
		ContinuousQueryManager: cq,
		BenchMark:              test,
	}

	return s, nil
}

func (s *Server) Start() error {

	go s.Http.Start()
	go s.RetentionPolicyManager.Start()
	go s.ContinuousQueryManager.Start()

	s.Log.Infof("Started server")

	go s.BenchMark.Start()

	return nil
}

func (s *Server) Stop() error {

	s.Log.Infof("Stopping server")

	s.Http.Stop()
	s.RetentionPolicyManager.Stop()
	s.ContinuousQueryManager.Stop()

	s.Log.Infof("All services stopped")

	s.Log.Infof("Closing database connection")
	s.mgo.Close()
	s.Log.Infof("Database closed")

	return nil
}
