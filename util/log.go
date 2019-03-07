package util

import (
	"fmt"
	"io/ioutil"

	"github.com/sirupsen/logrus"
)

// Logger is a wrapper on a logrus.FieldLogger, meant to ease use
type Logger interface {
	Infof(string, ...interface{})
	Errorf(string, ...interface{}) error
	Fatalf(string, ...interface{})
}

type logger struct {
	log logrus.FieldLogger
}

func (l logger) Infof(tmpl string, args ...interface{}) {
	l.log.Infof(tmpl, args...)
}

func (l logger) Errorf(tmpl string, args ...interface{}) error {
	err := fmt.Errorf(tmpl, args...)
	l.log.Errorf(tmpl, args...)
	return err
}

func (l logger) Fatalf(tmpl string, args ...interface{}) {
	l.log.Fatalf(tmpl, args...)
}

// NewLogger returns a util.Logger, with pairs of strings matched for fields
func NewLogger(fields ...string) Logger {
	if len(fields)%2 != 0 {
		panic(fmt.Errorf("Fields length must be a multiple of two"))
	}

	var fieldsMap logrus.Fields
	fieldsMap = logrus.Fields(make(map[string]interface{}))
	for i := 0; i < len(fields)-1; i += 2 {
		fieldsMap[fields[i]] = fields[i+1]
	}

	return logger{log: logrus.WithFields(fieldsMap)}
}

// DisableLogging reroutes all created loggers to a nil output
func DisableLogging() {
	logrus.SetOutput(ioutil.Discard)
}
