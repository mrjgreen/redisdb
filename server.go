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
	HTTP                   *HTTPListener
	RetentionPolicyManager *RetentionPolicyManager
	ContinuousQueryManager *ContinuousQueryManager
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

	configDb := mgoSession.DB("config")

	retention := &RetentionPolicyManager{
		Conn:          configDb,
		Store:         store,
		CheckInterval: c.Retention.CheckInterval,
		Log:           log,
	}

	cq := &ContinuousQueryManager{
		Conn:            configDb,
		Store:           store,
		ComputeInterval: c.ContinuousQuery.ComputeInterval,
		Log:             log,
	}

	http := &HTTPListener{
		BindAddress: c.HTTP.Port,
		Store:       store,
		RetentionPolicyManager: retention,
		ContinuousQueryManager: cq,
		Log: log,
	}

	s := &Server{
		mgo:   mgoSession,
		Store: store,
		Log:   log,
		HTTP:  http,
		RetentionPolicyManager: retention,
		ContinuousQueryManager: cq,
	}

	return s, nil
}

func (s *Server) Start() error {

	go s.HTTP.Start()
	go s.RetentionPolicyManager.Start()
	go s.ContinuousQueryManager.Start()

	s.Log.Infof("Started server")

	return nil
}

func (s *Server) Stop() error {

	s.Log.Infof("Stopping server")

	s.HTTP.Stop()
	s.RetentionPolicyManager.Stop()
	s.ContinuousQueryManager.Stop()

	s.Log.Infof("All services stopped")

	s.Log.Infof("Closing database connection")
	s.mgo.Close()
	s.Log.Infof("Database closed")

	return nil
}
