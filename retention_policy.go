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
	Prefix string
	Log log.Logger
	CheckInterval string
}

type RetentionPolicy struct {
	Name string
	TimeSeconds uint64
}

func (self *RetentionPolicyManager) AddRetentionPolicy(policy RetentionPolicy){

	timestr := strconv.FormatUint(policy.TimeSeconds, 10)

	self.Conn.HSet(self.Prefix + "config:retention", policy.Name, timestr)
}

func (self *RetentionPolicyManager) ApplyPolicy(policy RetentionPolicy){

	self.Log.Info(fmt.Sprintf("Applying retention policy to '%s'. Removing records older than %d seconds", policy.Name, policy.TimeSeconds))
}

func (self *RetentionPolicyManager) GetRetentionPolicies() []RetentionPolicy {

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

	self.AddRetentionPolicy(RetentionPolicy{"events:*", uint64(120)})

	go func(){
		for {
			self.Log.Info("Checking retention policies after " + self.CheckInterval)

			policies := self.GetRetentionPolicies()

			for _, policy := range policies{
				self.ApplyPolicy(policy)
			}

			time.Sleep(duration)
		}
	}()
}
