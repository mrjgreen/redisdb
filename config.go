package main

type RedisConfig struct {
	Host 		string	`toml:"host"`
	Sentinel 	bool	`toml:"sentinel"`
	Auth 		string	`toml:"auth"`
	KeyPrefix 	string	`toml:"key-prefix"`
}

type HTTPConfig struct {
	Enabled		bool	`toml:"enabled"`
	Port 		string	`toml:"port"`
}

type TCPConfig struct {
	Enabled		bool	`toml:"enabled"`
	Port 		string	`toml:"port"`
}

type RetentionConfig struct {
	CheckInterval	string `toml:"check-interval"`
}

type ContinuousQueryConfig struct {
	ComputeInterval	string `toml:"compute-interval"`
}

type LogConfig struct {
	Enabled bool	`toml:"enabled"`
	Level	string	`toml:"level"`
	Log		string	`toml:"log"`
}

type Config struct {
	Redis      	*RedisConfig       	`toml:"redis"`
	Log       	*LogConfig       	`toml:"log"`
	Retention  	*RetentionConfig   	`toml:"retention"`
	HTTP		*HTTPConfig			`toml:"http"`
	TCP			*TCPConfig           `toml:"tcp"`
	ContinuousQuery *ContinuousQueryConfig `toml:"continuous_queries"`
}

// NewConfig returns an instance of Config with reasonable defaults.
func NewConfig() *Config {

	c := &Config{
		Redis : &RedisConfig{
			Host : "localhost:6379",
			KeyPrefix : "reduxdb:",
		},
		Log : 	&LogConfig{
			Enabled : true,
			Level : "info",
		},
		Retention : &RetentionConfig{
			CheckInterval :	"2m",
		},
		ContinuousQuery : &ContinuousQueryConfig{
			ComputeInterval : "10s",
		},
		HTTP : &HTTPConfig{
			Enabled : true,
			Port : "8086",
		},
		TCP : &TCPConfig{
			Enabled : true,
			Port : "6086",
		},
	}

	return c
}
