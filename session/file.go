package session

import (
	"errors"
)

type FileSession struct {

}

func NewFileSession(filename string) (Session, error) {
	fileSession := FileSession { }
	return fileSession, nil
}

func (f FileSession) Close() error {
	return nil
}

func (f FileSession) Write(string) error {
	return errors.New("unimplemented")
}
