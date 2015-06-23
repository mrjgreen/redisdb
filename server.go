package main

import (
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
	BenchMark *BenchMark
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c *Config) (*Server, error) {

	client := redis.NewClient(&redis.Options{
		Addr:     c.Redis.Host,
		Password: c.Redis.Auth,
		DB:       c.Redis.Database,
	})

	log, err := NewLogger(c.Log)

	if err != nil {
		return nil, err
	}

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

	test := &BenchMark{
		Store : store,
		Log: log,
		RetentionPolicyManager: retention,
		ContinuousQueryManager: cq,
	}

	s := &Server{
		Store : store,
		Log : log,
		Http : http,
		RetentionPolicyManager : retention,
		ContinuousQueryManager : cq,
		BenchMark : test,
	}

	return s, nil
}

func (s *Server) Start() error {

	go s.Http.Start()
	go s.RetentionPolicyManager.Start()
	go s.ContinuousQueryManager.Start()

	s.Log.Info("Started server")

	s.RetentionPolicyManager.Delete("click:raw:c:*")

	s.RetentionPolicyManager.Add(RetentionPolicy{"click:raw:c:*", float64(20 * 60)})

	s.ContinuousQueryManager.Add(ContinuousQuery{
		SourceSeries : "click:raw:c:*",
		TargetSeries : "click:10m:c:*", // The glob pattern of source will be mapped onto the target
		Granularity : "10m",
		Query : SeriesSearch{
			Values: SearchValues{
				"count" : SearchValue{Type:"COUNT"},
			},
			Group : SearchGroupBy{Enabled: true},
		},
	})

	go s.BenchMark.Start()

	return nil
}

func (s *Server) Stop() error {

	s.Log.Info("Stopped server")

	return  nil
}
