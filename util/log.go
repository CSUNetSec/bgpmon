package util

import (
	"fmt"
	"github.com/sirupsen/logrus"
)

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
