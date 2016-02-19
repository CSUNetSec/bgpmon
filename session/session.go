package session

import (
)

type IOSessions struct {
	In  []Session
	Out []Session
}

type Session interface {
	Close() error
	Write(string) error
}
