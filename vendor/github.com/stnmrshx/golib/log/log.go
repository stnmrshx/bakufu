/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package log

import (
	"errors"
	"fmt"
	"log/syslog"
	"os"
	"runtime/debug"
	"time"
)

type LogLevel int

func (this LogLevel) String() string {
	switch this {
	case FATAL:
		return "FATAL"
	case CRITICAL:
		return "CRITICAL"
	case ERROR:
		return "ERROR"
	case WARNING:
		return "WARNING"
	case NOTICE:
		return "NOTICE"
	case INFO:
		return "INFO"
	case DEBUG:
		return "DEBUG"
	}
	return "unknown"
}

func LogLevelFromString(logLevelName string) (LogLevel, error) {
	switch logLevelName {
	case "FATAL":
		return FATAL, nil
	case "CRITICAL":
		return CRITICAL, nil
	case "ERROR":
		return ERROR, nil
	case "WARNING":
		return WARNING, nil
	case "NOTICE":
		return NOTICE, nil
	case "INFO":
		return INFO, nil
	case "DEBUG":
		return DEBUG, nil
	}
	return 0, fmt.Errorf("Unknown LogLevel name: %+v", logLevelName)
}

const (
	FATAL LogLevel = iota
	CRITICAL
	ERROR
	WARNING
	NOTICE
	INFO
	DEBUG
)

const TimeFormat = "2006-01-02 15:04:05"

var globalLogLevel LogLevel = DEBUG
var printStackTrace bool = false

var syslogLevel LogLevel = ERROR
var syslogWriter *syslog.Writer

func SetPrintStackTrace(shouldPrintStackTrace bool) {
	printStackTrace = shouldPrintStackTrace
}

func SetLevel(logLevel LogLevel) {
	globalLogLevel = logLevel
}

func GetLevel() LogLevel {
	return globalLogLevel
}

func EnableSyslogWriter(tag string) (err error) {
	syslogWriter, err = syslog.New(syslog.LOG_ERR, tag)
	if err != nil {
		syslogWriter = nil
	}
	return err
}

func SetSyslogLevel(logLevel LogLevel) {
	syslogLevel = logLevel
}

func logFormattedEntry(logLevel LogLevel, message string, args ...interface{}) string {
	if logLevel > globalLogLevel {
		return ""
	}
	msgArgs := fmt.Sprintf(message, args...)
	entryString := fmt.Sprintf("%s %s %s", time.Now().Format(TimeFormat), logLevel, msgArgs)
	fmt.Fprintln(os.Stderr, entryString)

	if syslogWriter != nil {
		go func() error {
			if logLevel > syslogLevel {
				return nil
			}
			switch logLevel {
			case FATAL:
				return syslogWriter.Emerg(msgArgs)
			case CRITICAL:
				return syslogWriter.Crit(msgArgs)
			case ERROR:
				return syslogWriter.Err(msgArgs)
			case WARNING:
				return syslogWriter.Warning(msgArgs)
			case NOTICE:
				return syslogWriter.Notice(msgArgs)
			case INFO:
				return syslogWriter.Info(msgArgs)
			case DEBUG:
				return syslogWriter.Debug(msgArgs)
			}
			return nil
		}()
	}
	return entryString
}

func logEntry(logLevel LogLevel, message string, args ...interface{}) string {
	entryString := message
	for _, s := range args {
		entryString += fmt.Sprintf(" %s", s)
	}
	return logFormattedEntry(logLevel, entryString)
}

func logErrorEntry(logLevel LogLevel, err error) error {
	if err == nil {
		return nil
	}
	entryString := fmt.Sprintf("%+v", err)
	logEntry(logLevel, entryString)
	if printStackTrace {
		debug.PrintStack()
	}
	return err
}

func Debug(message string, args ...interface{}) string {
	return logEntry(DEBUG, message, args...)
}

func Debugf(message string, args ...interface{}) string {
	return logFormattedEntry(DEBUG, message, args...)
}

func Info(message string, args ...interface{}) string {
	return logEntry(INFO, message, args...)
}

func Infof(message string, args ...interface{}) string {
	return logFormattedEntry(INFO, message, args...)
}

func Notice(message string, args ...interface{}) string {
	return logEntry(NOTICE, message, args...)
}

func Noticef(message string, args ...interface{}) string {
	return logFormattedEntry(NOTICE, message, args...)
}

func Warning(message string, args ...interface{}) error {
	return errors.New(logEntry(WARNING, message, args...))
}

func Warningf(message string, args ...interface{}) error {
	return errors.New(logFormattedEntry(WARNING, message, args...))
}

func Error(message string, args ...interface{}) error {
	return errors.New(logEntry(ERROR, message, args...))
}

func Errorf(message string, args ...interface{}) error {
	return errors.New(logFormattedEntry(ERROR, message, args...))
}

func Errore(err error) error {
	return logErrorEntry(ERROR, err)
}

func Critical(message string, args ...interface{}) error {
	return errors.New(logEntry(CRITICAL, message, args...))
}

func Criticalf(message string, args ...interface{}) error {
	return errors.New(logFormattedEntry(CRITICAL, message, args...))
}

func Criticale(err error) error {
	return logErrorEntry(CRITICAL, err)
}

func Fatal(message string, args ...interface{}) error {
	logEntry(FATAL, message, args...)
	os.Exit(1)
	return errors.New(logEntry(CRITICAL, message, args...))
}

func Fatalf(message string, args ...interface{}) error {
	logFormattedEntry(FATAL, message, args...)
	os.Exit(1)
	return errors.New(logFormattedEntry(CRITICAL, message, args...))
}

func Fatale(err error) error {
	logErrorEntry(FATAL, err)
	os.Exit(1)
	return err
}
