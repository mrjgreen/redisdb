package main

import (
	"os"
	log "gopkg.in/inconshreveable/log15.v2"
)

func NewLogger(c *LogConfig) (log.Logger, error) {

	if !c.Enabled{
		return nil, nil
	}

	var l = log.New()

	level, err := log.LvlFromString(c.Level)

	if err != nil{
		return nil, err
	}

	l.SetHandler(log.LvlFilterHandler(level, log.StreamHandler(os.Stdout, log.TerminalFormat())))

	return l, nil
}
