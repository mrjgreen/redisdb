package main

type RedisConfig struct {
	Host 		string	`toml:"host"`
	//Sentinel 	bool	`toml:"sentinel"`
	Auth 		string	`toml:"auth"`
	KeyPrefix 	string	`toml:"key-prefix"`
	Database	string  `toml:"database"`
}

type HTTPConfig struct {
	Enabled		bool	`toml:"enabled"`
	Port 		string	`toml:"port"`
}

type RetentionConfig struct {
	Enabled 		bool	`toml:"enabled"`
	CheckInterval	string `toml:"check-interval"`
}

type ContinuousQueryConfig struct {
	Enabled 		bool	`toml:"enabled"`
	ComputeInterval	string `toml:"compute-interval"`
}

type LogConfig struct {
	Enabled bool	`toml:"enabled"`
	Level	string	`toml:"level"`
	//Log		string	`toml:"log"`
}

type Config struct {
	Redis      	*RedisConfig       	`toml:"redis"`
	Log       	*LogConfig       	`toml:"log"`
	Retention  	*RetentionConfig   	`toml:"retention"`
	HTTP		*HTTPConfig			`toml:"http"`
	ContinuousQuery *ContinuousQueryConfig `toml:"continuous_queries"`
}

// NewConfig returns an instance of Config with reasonable defaults.
func NewConfig() *Config {

	c := &Config{
		Redis : &RedisConfig{
			Host : "localhost:6379",
			KeyPrefix : "reduxdb:",
			Database : 0,
		},
		Log : 	&LogConfig{
			Enabled : true,
			Level : "info",
		},
		Retention : &RetentionConfig{
			Enabled : true,
			CheckInterval :	"10s",
		},
		ContinuousQuery : &ContinuousQueryConfig{
			Enabled : true,
			ComputeInterval : "10s",
		},
		HTTP : &HTTPConfig{
			Enabled : true,
			Port : "localhost:8086",
		},
	}

	return c
}

func (self *Config) PopulateFromFile(path string) error{

	_, err := toml.DecodeFile(path, self)

	return err
}

