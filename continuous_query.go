package main

import (
	"time"
	log "gopkg.in/inconshreveable/log15.v2"
	redis "gopkg.in/redis.v3"
)

type ContinuousQueryManager struct{
	Conn *redis.Client
	Prefix string
	Log log.Logger
	ComputeInterval string
}

func (self *ContinuousQueryManager) AddContinuousQuery(){

}

func (self *ContinuousQueryManager) RemoveContinuousQuery(){

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
