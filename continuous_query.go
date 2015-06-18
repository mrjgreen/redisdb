package main

import (
	"time"
	log "gopkg.in/inconshreveable/log15.v2"
	redis "gopkg.in/redis.v3"
)

type ContinuousQueryManager struct{
	Conn *redis.Client
	Prefix string
	Store SeriesStore
	Log log.Logger
	ComputeInterval string
}

func (self *ContinuousQueryManager) Add(){

}

func (self *ContinuousQueryManager) Delete(){

}

func (self *ContinuousQueryManager) Apply(){

	// Calculate last two time periods based on granularity

	// Perform search and group by

	// Using multi exec
	// Delete from store where time between X AND X (same time stamp inclusive should only delete one range)
	// Insert into new key

}

func (self *ContinuousQueryManager) Start(){

	var duration,_ = time.ParseDuration(self.ComputeInterval);

	go func(){
		for {
			// Read continuous query configurations
			self.Log.Info("Checking continuous queries after " + self.ComputeInterval)

			time.Sleep(duration)
		}
	}()
}
