package util

import (
	"log"
	"io"
	"os"
	"fmt"
)

const (
	LogLevelError int = iota
	// LogLevelInfo log messages for info
	LogLevelInfo 
	// LogLevelDebug log messages for info and debug
	LogLevelDebug
	// LogLevelTrace log messages info, debug and trace
	LogLevelTrace
)

var LOG_FILE io.Writer = os.Stdout

type CommonLogger struct {
	logLevel int
	logger *log.Logger
}

func NewLogger (module string, logLevel int) *CommonLogger{
	l := log.New(LOG_FILE, module, log.Lmicroseconds)
	return &CommonLogger {logLevel, l}		
}

func (l *CommonLogger) logMsgf (level int, format string, v...interface{}) {
	if (l.logLevel >= level) {
		msg := fmt.Sprintf(format, v)
		l.logger.Println(msg)
	}
}

func (l *CommonLogger) logMsg (level int, msg string) {
	if (l.logLevel >= level) {
		l.logger.Println(msg)
	}
}

func (l *CommonLogger) Infof (format string, v...interface{}) {
	l.logMsgf(LogLevelInfo, "[INFO] "+format, v)
}

func (l *CommonLogger) Debugf (format string, v...interface{}) {
	l.logMsgf(LogLevelDebug, "[DEBUG] "+format, v)
}

func (l *CommonLogger) Tracef (format string, v...interface{}) {
	l.logMsgf(LogLevelTrace, "[TRACE] "+format, v)
}

func (l *CommonLogger) Errorf (format string, v...interface{}) {
	l.logMsgf(LogLevelTrace, "[ERROR] "+format, v)
}

func (l *CommonLogger) Info (msg string) {
	l.logMsg(LogLevelInfo, "[INFO] "+msg)
}

func (l *CommonLogger) Debug (msg string) {
	l.logMsg(LogLevelDebug, "[DEBUG] "+msg)
}

func (l *CommonLogger) Trace (msg string) {
	l.logMsgf(LogLevelTrace, "[TRACE] "+msg)
}

func (l *CommonLogger) Error (msg string) {
	l.logMsg(LogLevelTrace, "[ERROR] "+msg)
}