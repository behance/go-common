//log is a wrapper for go-logrus forked from https://github.com/Sirupsen/logrus
// It servers 2 main purposes:
// 1 - it eliminates the need for awkward .WithFields calls by intelligently creating fields
//     based on the number and positions of paremeters to the Warn, Error, Fatal and Info calls.
// 2 - it adds stack info to ever call for easier debugging
package log

import (
	"os"
	"runtime"
	"strconv"
	"strings"

	log "github.com/behance/go-logrus"
)

const (
	DebugLevel = log.DebugLevel
	InfoLevel  = log.InfoLevel
	WarnLevel  = log.WarnLevel
	ErrorLevel = log.ErrorLevel
	PanicLevel = log.PanicLevel
)

type errorInfo struct {
	err      error
	funcName string
	file     string
	line     int
}

func SetLevel(level log.Level) {
	log.SetLevel(level)
}

func SetOutput(logFile *os.File) {
	log.SetOutput(logFile)
}

func AlwaysShowColors(tf bool) {
	log.SetFormatter(&log.TextFormatter{ForceColors: tf})
}

func Debug(errs ...interface{}) {
	msg, fields := separateMsgAndFields(errs...)
	buildLogEntry(fields...).Debug(msg)
}
func Debugf(str string, vars ...interface{}) {
	if len(vars) >= 1 {
		buildLogEntry().Debugf(str, vars...)
	} else {
		buildLogEntry().Debug(nil)
	}
}
func Warn(errs ...interface{}) {
	msg, fields := separateMsgAndFields(errs...)
	buildLogEntry(fields...).Warn(msg)
}
func Warnf(str string, vars ...interface{}) {
	if len(vars) >= 1 {
		buildLogEntry().Warnf(str, vars...)
	} else {
		buildLogEntry().Warn(nil)
	}
}
func Error(errs ...interface{}) {
	msg, fields := separateMsgAndFields(errs...)
	buildLogEntry(fields...).Error(msg)
}
func Errorf(str string, vars ...interface{}) {
	if len(vars) >= 1 {
		buildLogEntry().Errorf(str, vars...)
	} else {
		buildLogEntry().Error(nil)
	}
}
func Fatal(errs ...interface{}) {
	msg, fields := separateMsgAndFields(errs...)
	buildLogEntry(fields...).Fatal(msg)
}
func Info(errs ...interface{}) {
	msg, fields := separateMsgAndFields(errs...)
	buildLogEntry(fields...).Info(msg)
}
func Infof(str string, vars ...interface{}) {
	if len(vars) >= 1 {
		buildLogEntry().Infof(str, vars...)
	} else {
		buildLogEntry().Info(nil)
	}
}

func separateMsgAndFields(things ...interface{}) (msg interface{}, fields []interface{}) {
	if len(things)%2 == 1 && len(things) > 1 {
		fields = things[1:]
		msg = things[0]
	} else if len(things) == 1 {
		fields = nil
		msg = things[0]
	} else {
		fields = things
		msg = ""
	}
	return
}

func buildLogEntry(msgs ...interface{}) *log.Entry {
	errInfo := getCodeLocationInfo(3)
	higherErrInfo := getCodeLocationInfo(4)
	logEntry := log.WithFields(log.Fields{
		"caller": chopDirs(errInfo.funcName) + ":" + strconv.Itoa(errInfo.line) + " | " + chopDirs(higherErrInfo.funcName) + ":" + strconv.Itoa(higherErrInfo.line),
		"file":   chopDirs(errInfo.file),
	})
	for i := 0; i < len(msgs)-1; i = i + 2 {
		if str, ok := msgs[i].(string); ok {
			logEntry.Data[str] = msgs[i+1]
		} else {
			//object is not stringable, treat each element separately
			logEntry.Data["msg-"+strconv.Itoa(i)] = msgs[i]
			logEntry.Data["msg-"+strconv.Itoa(i+1)] = msgs[i+1]
		}
	}
	return logEntry
}

func getCodeLocationInfo(depth int) *errorInfo {
	pc, file, line, _ := runtime.Caller(depth)
	me := runtime.FuncForPC(pc)
	errInfo := &errorInfo{
		funcName: me.Name(),
		file:     file,
		line:     line,
	}
	return errInfo
}

func chopDirs(longPath string) string {
	return longPath[strings.LastIndexAny(longPath, "/")+1:]
}
