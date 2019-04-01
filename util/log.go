package util

import (
	"fmt"
	"io/ioutil"

	"github.com/sirupsen/logrus"
)

// Logger is a simple wrapper around logrus.FieldLogger which eases use
// and adds some functionality.
type Logger struct {
	log logrus.FieldLogger
}

// Infof prints to the screen with INFO priority. This isn't meant to
// be used for error messages.
func (l Logger) Infof(tmpl string, args ...interface{}) {
	l.log.Infof(tmpl, args...)
}

// Errorf prints to the screen with ERROR priority, and returns an
// error to be handled. It is a combination of log.Errorf and fmt.Errorf.
func (l Logger) Errorf(tmpl string, args ...interface{}) error {
	err := fmt.Errorf(tmpl, args...)
	l.log.Errorf(tmpl, args...)
	return err
}

// Fatalf prints to the screen with FATAL priority, and exits the
// application.
func (l Logger) Fatalf(tmpl string, args ...interface{}) {
	l.log.Fatalf(tmpl, args...)
}

// NewLogger returns a util.Logger, with pairs of strings matched for fields.
func NewLogger(fields ...string) Logger {
	if len(fields)%2 != 0 {
		fields = []string{"Logger Fields", "invalid"}
	}

	var fieldsMap logrus.Fields
	fieldsMap = logrus.Fields(make(map[string]interface{}))
	for i := 0; i < len(fields)-1; i += 2 {
		fieldsMap[fields[i]] = fields[i+1]
	}

	return Logger{log: logrus.WithFields(fieldsMap)}
}

// DisableLogging reroutes all created loggers to a nil output.
func DisableLogging() {
	logrus.SetOutput(ioutil.Discard)
}
