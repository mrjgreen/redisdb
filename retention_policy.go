package main

import (
	"time"

	"github.com/mrjgreen/redisdb/utils"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type RetentionPolicyManager struct {
	Conn          *mgo.Database
	Store         SeriesStore
	Log           utils.Logger
	CheckInterval string
	stop          chan struct{}
}

type RetentionPolicy struct {
	Name        string
	TimeSeconds time.Duration
}

func (self *RetentionPolicyManager) Add(policy RetentionPolicy) {

	self.Log.Infof("Adding retention policy '%s' with retention %s seconds", policy.Name, policy.TimeSeconds)

	self.Conn.C("retention_policies").Insert(policy)
}

func (self *RetentionPolicyManager) Delete(name string) {

	self.Log.Infof("Removing retention policy '%s'", name)

	self.Conn.C("retention_policies").RemoveAll(bson.M{
		"name": name,
	})
}

func (self *RetentionPolicyManager) ApplyPolicy(policy RetentionPolicy) {

	items := self.Store.List(policy.Name)

	for _, series := range items {

		self.Log.Infof("Applying retention policy '%s' to '%s'. Removing records older than %s", policy.Name, series.Name, policy.TimeSeconds)

		search := NewRangeBefore(policy.TimeSeconds)

		self.Store.Delete(series.Name, search)
	}
}

func (self *RetentionPolicyManager) List() []RetentionPolicy {

	var policies []RetentionPolicy

	self.Conn.C("retention_policies").Find(nil).All(&policies)

	return policies
}

func (self *RetentionPolicyManager) Start() {

	if self.stop != nil {
		return
	}

	self.stop = make(chan struct{})

	var duration, _ = time.ParseDuration(self.CheckInterval)

	for {
		select {
		case <-self.stop:
			return
		case <-time.After(duration):
			self.Log.Infof("Checking retention policies after %s", self.CheckInterval)

			policies := self.List()

			for _, policy := range policies {
				self.ApplyPolicy(policy)
			}

			time.Sleep(duration)
		}
	}
}

// Close closes the underlying listener.
func (self *RetentionPolicyManager) Stop() {
	if self.stop == nil {
		return
	}

	close(self.stop)
	self.stop = nil

	self.Log.Infof("Stopped retention policy manager")
}
