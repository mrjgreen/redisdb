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
	TimeSeconds float64
}

func (self *RetentionPolicyManager) Add(policy RetentionPolicy){

	timestr := strconv.FormatFloat(policy.TimeSeconds, 'f', -1, 64)

	self.Log.Info(fmt.Sprintf("Adding retention policy '%s' with retention %s seconds", policy.Name, timestr))

	self.Conn.HSet(self.Prefix + "config:retention", policy.Name, timestr)
}

func (self *RetentionPolicyManager) Delete(name string){

	self.Log.Info(fmt.Sprintf("Removing retention policy '%s'", name))

	self.Conn.HDel(self.Prefix + "config:retention", name)
}

func (self *RetentionPolicyManager) ApplyPolicy(policy RetentionPolicy){

	items := self.Store.List(policy.Name)

	for _, series := range items {

		self.Log.Info(fmt.Sprintf("Applying retention policy '%s' to '%s'. Removing records older than %f seconds", policy.Name, series.Name, policy.TimeSeconds))

		search := NewRangeBefore(policy.TimeSeconds)

		self.Store.Delete(series.Name, search)
	}
}

func (self *RetentionPolicyManager) List() []RetentionPolicy {

	items := self.Conn.HGetAllMap(self.Prefix + "config:retention")

	var policies = make([]RetentionPolicy, 0)

	for name, time := range items.Val(){

		timeflt,_ := strconv.ParseFloat(time, 64)

		policy := RetentionPolicy{
			Name : name,
			TimeSeconds : timeflt,
		}

		policies = append(policies, policy)
	}

	return policies
}

func (self *RetentionPolicyManager) Start(){

	var duration,_ = time.ParseDuration(self.CheckInterval);

	for {
		self.Log.Info("Checking retention policies after " + self.CheckInterval)

		policies := self.List()

		for _, policy := range policies{
			self.ApplyPolicy(policy)
		}

		time.Sleep(duration)
	}
}
