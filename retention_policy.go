package main

import (
	"errors"
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
	Name        string        `json:"name"`
	TimeSeconds time.Duration `json:"time"`
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

func (self *RetentionPolicyManager) Start() error {

	var duration, err = time.ParseDuration(self.CheckInterval)

	if err != nil {
		return err
	}

	if self.stop != nil {
		return errors.New("Retention policy manager is already running")
	}

	self.stop = make(chan struct{})

	self.Log.Infof("Started retention policy manager running every %s", duration)

	for {
		select {
		case <-self.stop:
			return nil
		case <-time.After(duration):
			self.Log.Infof("Checking retention policies after %s", self.CheckInterval)

			policies := self.List()

			for _, policy := range policies {
				self.ApplyPolicy(policy)
			}
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
