package session

import (
)

type IOSessions struct {
	In  []Session
	Out []Session
}

type Session interface {
	Write(string) error
}
