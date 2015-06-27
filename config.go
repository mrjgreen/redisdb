package main

import (
	"github.com/BurntSushi/toml"
)

type MongoConfig struct {
	Host string `toml:"host"`
}

type HTTPConfig struct {
	Enabled bool   `toml:"enabled"`
	Port    string `toml:"port"`
}

type RetentionConfig struct {
	Enabled       bool   `toml:"enabled"`
	CheckInterval string `toml:"check-interval"`
}

type ContinuousQueryConfig struct {
	Enabled         bool   `toml:"enabled"`
	ComputeInterval string `toml:"compute-interval"`
}

type LogConfig struct {
	Level string `toml:"level"`
}

type Config struct {
	Mongo           *MongoConfig           `toml:"mongo"`
	Log             *LogConfig             `toml:"log"`
	Retention       *RetentionConfig       `toml:"retention"`
	HTTP            *HTTPConfig            `toml:"http"`
	ContinuousQuery *ContinuousQueryConfig `toml:"continuous_queries"`
}

// NewConfig returns an instance of Config with reasonable defaults.
func NewConfig() *Config {

	c := &Config{
		Mongo: &MongoConfig{
			Host: "localhost",
		},
		Log: &LogConfig{
			Level: "info",
		},
		Retention: &RetentionConfig{
			Enabled:       true,
			CheckInterval: "10s",
		},
		ContinuousQuery: &ContinuousQueryConfig{
			Enabled:         true,
			ComputeInterval: "5s",
		},
		HTTP: &HTTPConfig{
			Enabled: true,
			Port:    ":8086",
		},
	}

	return c
}

func (self *Config) PopulateFromFile(path string) error {

	_, err := toml.DecodeFile(path, self)

	return err
}
