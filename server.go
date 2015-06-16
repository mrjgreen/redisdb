package main

import (
	"fmt"
)

// Server represents a container for the metadata and storage data and services.
// It is built using a Config and it manages the startup and shutdown of all
// services in the proper order.
type Server struct {

}

// NewServer returns a new instance of Server built from a config.
func NewServer(c *Config) (*Server, error) {
	// Construct base meta store and data store.
	s := &Server{

	}

	fmt.Printf("%v", c.HTTP.Enabled)

	return s, nil
}


func (s *Server) Open() error {

	fmt.Println("Opened server")

	return  nil
}
