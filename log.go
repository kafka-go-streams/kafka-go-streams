package streams

import (
	log "github.com/sirupsen/logrus"
)

type LogWrapper struct {
	logger *log.Logger
}

func (l *LogWrapper) Logf(level log.Level, format string, args ...interface{}) {
	if l != nil && l.logger != nil {
		l.logger.Logf(level, format, args...)
	}
}

func (l *LogWrapper) Debugf(format string, args ...interface{}) {
	l.Logf(log.DebugLevel, format, args...)
}
