package main

import (
	"fmt"
	"runtime"
	"github.com/BurntSushi/toml"
)

type Command struct {

}

func NewRunCommand() *Command {
	return &Command{
		//closing: make(chan struct{})
	}
}

// Run parses the config from args and runs the server.
func (cmd *Command) Run(config_path string) error {

	// Set parallelism.
	runtime.GOMAXPROCS(runtime.NumCPU())
	fmt.Printf("GOMAXPROCS set to %d\n", runtime.GOMAXPROCS(0))

	// Parse config
	config, err := cmd.ParseConfig(config_path)
	if err != nil {
		return fmt.Errorf("parse config: %s", err)
	}

	// Create server from config and start it.
	s, err := NewServer(config)
	if err != nil {
		return fmt.Errorf("create server: %s", err)
	}

	if err := s.Start(); err != nil {
		return fmt.Errorf("open server: %s", err)
	}

	return nil
}


// ParseConfig parses the config at path.
// Returns a demo configuration if path is blank.
func (cmd *Command) ParseConfig(path string) (*Config, error) {

	config := NewConfig()

	// Use demo configuration if no config path is specified.
	if path == "" {
		fmt.Println("no configuration provided, using default settings")
	}else{
		fmt.Printf("using configuration at: %s\n", path)

		if _, err := toml.DecodeFile(path, &config); err != nil {
			return nil, err
		}
	}

	return config, nil
}
