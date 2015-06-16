package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	if err := start(os.Args[1:]...); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}


// Run determines and runs the command specified by the CLI args.
func start(args ...string) error {

	var config_path string

	if(len(args) > 0){
		config_path = args[0]
	}

	cmd := NewRunCommand()

	if err := cmd.Run(config_path); err != nil {
		return err
	}

	// Wait indefinitely.
	//<-(chan struct{})(nil)

	return nil
}
