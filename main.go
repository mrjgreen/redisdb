package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

func main() {

	rand.Seed(time.Now().UnixNano())

	// Set parallelism.
	runtime.GOMAXPROCS(runtime.NumCPU())
	fmt.Printf("Using %d CPU cores\n", runtime.GOMAXPROCS(0))

	if err := start(os.Args[1:]...); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Run determines and runs the command specified by the CLI args.
func start(args ...string) error {

	config := NewConfig()

	if len(args) > 0 {

		fmt.Printf("Loading configuration from file: %s\n", args[0])

		if err := config.PopulateFromFile(args[0]); err != nil {
			return err
		}
	}

	s, err := NewServer(config)

	if err != nil {
		return err
	}

	if err := s.Start(); err != nil {
		return err
	}

	defer s.Stop()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Signal(syscall.SIGTERM))
	<-ch

	return nil
}
