package utils

import (
	"fmt"
	"os"

	log "gopkg.in/inconshreveable/log15.v2"
)

// Logging interface. Different services might want to use a different logger
// for logging (possible use-case: file vs tty logging) but we want to
// constrain what methods are available
type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Printf(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Critf(format string, args ...interface{})
}

// Logrus logger
type Log struct {
	Logger
	logService log.Logger
}

func (l *Log) Debugf(format string, args ...interface{}) {
	l.logService.Debug(fmt.Sprintf(format, args...))
}

func (l *Log) Infof(format string, args ...interface{}) {
	l.logService.Info(fmt.Sprintf(format, args...))
}

func (l *Log) Warnf(format string, args ...interface{}) {
	l.logService.Warn(fmt.Sprintf(format, args...))
}

func (l *Log) Errorf(format string, args ...interface{}) {
	l.logService.Error(fmt.Sprintf(format, args...))
}

func (l *Log) Critf(format string, args ...interface{}) {
	l.logService.Crit(fmt.Sprintf(format, args...))
}

func NewLogger(levelstr string) (*Log, error) {

	var l = log.New()

	level, err := log.LvlFromString(levelstr)

	l.SetHandler(log.LvlFilterHandler(level, log.StreamHandler(os.Stdout, log.TerminalFormat())))

	return &Log{logService: l}, err
}
