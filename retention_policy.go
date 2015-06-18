package main

import (
	"time"
	"fmt"
	"strconv"
	log "gopkg.in/inconshreveable/log15.v2"
	redis "gopkg.in/redis.v3"
)

type RetentionPolicyManager struct{
	Conn *redis.Client
	Store SeriesStore
	Prefix string
	Log log.Logger
	CheckInterval string
}

type RetentionPolicy struct {
	Name string
	TimeSeconds uint64
}

func (self *RetentionPolicyManager) Add(policy RetentionPolicy){

	timestr := strconv.FormatUint(policy.TimeSeconds, 10)

	self.Log.Info(fmt.Sprintf("Adding retention policy '%s' with retention %s seconds", policy.Name, timestr))

	self.Conn.HSet(self.Prefix + "config:retention", policy.Name, timestr)
}

func (self *RetentionPolicyManager) Delete(name string){

	self.Log.Info(fmt.Sprintf("Removing retention policy '%s'", name))

	self.Conn.HDel(self.Prefix + "config:retention", name)
}

func (self *RetentionPolicyManager) ApplyPolicy(policy RetentionPolicy){

	items := self.Store.ListSeries(policy.Name)

	for _, series := range items {

		self.Log.Info(fmt.Sprintf("Applying retention policy '%s' to '%s'. Removing records older than %d seconds", policy.Name, series.Name, policy.TimeSeconds))

		search := NewSearchOlderThan(policy.TimeSeconds)

		self.Store.Delete(series.Name, search)

		return
	}
}

func (self *RetentionPolicyManager) List() []RetentionPolicy {

	items := self.Conn.HGetAllMap(self.Prefix + "config:retention")

	var policies = make([]RetentionPolicy, 0)

	for name, time := range items.Val(){

		timeint,_ := strconv.ParseUint(time, 10, 64)

		policy := RetentionPolicy{
			Name : name,
			TimeSeconds : timeint,
		}

		policies = append(policies, policy)
	}

	return policies
}

func (self *RetentionPolicyManager) Start(){

	var duration,_ = time.ParseDuration(self.CheckInterval);

	go func(){
		for {
			self.Log.Info("Checking retention policies after " + self.CheckInterval)

			policies := self.List()

			for _, policy := range policies{
				self.ApplyPolicy(policy)
			}

			time.Sleep(duration)
		}
	}()
}
