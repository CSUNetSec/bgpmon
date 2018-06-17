package db

import (
	"github.com/sirupsen/logrus"
)

var (
	dblogger = logrus.WithField("system", "db")
)
