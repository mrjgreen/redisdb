package main

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"github.com/BurntSushi/toml"
)

type Command struct {
	closing chan struct{}

	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer

	Server *Server
}

func NewRunCommand() *Command {
	return &Command{
		closing: make(chan struct{}),
		Stdin:   os.Stdin,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
	}
}

// Run parses the config from args and runs the server.
func (cmd *Command) Run(config_path string) error {

	// Set parallelism.
	runtime.GOMAXPROCS(runtime.NumCPU())
	fmt.Fprintf(cmd.Stderr, "GOMAXPROCS set to %d\n", runtime.GOMAXPROCS(0))

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

	if err := s.Open(); err != nil {
		return fmt.Errorf("open server: %s", err)
	}
	cmd.Server = s

	return nil
}


// ParseConfig parses the config at path.
// Returns a demo configuration if path is blank.
func (cmd *Command) ParseConfig(path string) (*Config, error) {

	config := NewConfig()

	// Use demo configuration if no config path is specified.
	if path == "" {
		fmt.Fprintln(cmd.Stdout, "no configuration provided, using default settings")
	}else{
		fmt.Fprintf(cmd.Stdout, "using configuration at: %s\n", path)

		if _, err := toml.DecodeFile(path, &config); err != nil {
			return nil, err
		}
	}

	return config, nil
}
