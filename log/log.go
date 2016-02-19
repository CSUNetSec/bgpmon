package log

import (
	"io"
	"io/ioutil"
	"log"
	"os"
)

var (
	Debl *log.Logger
	Errl *log.Logger
)

func Init(debugOut string, errorOut string) (func() error, func() error, error) {
	debugFile, debugClose, err := parseOutputFile(debugOut)
	if err != nil {
		return nil, nil, err
	}
	Debl = log.New(debugFile, "Debug: ", log.Ldate|log.Ltime|log.Lshortfile)

	errorFile, errorClose, err := parseOutputFile(errorOut)
	if err != nil {
		return nil, nil, err
	}
	Errl = log.New(errorFile, "Error: ", log.Ldate|log.Ltime|log.Lshortfile)

	return debugClose, errorClose, nil
}

func parseOutputFile(streamType string) (io.Writer, func() error, error) {
	switch streamType {
	case "discard":
		return ioutil.Discard, func() error { return nil }, nil
	case "stderr":
		return os.Stderr, func() error { return nil }, nil
	case "stdout":
		return os.Stdout, func() error { return nil }, nil
	default:
		file, err := os.Create(streamType)
		if err != nil {
			return nil, func() error { return nil }, err
		}
		return file, file.Close, nil
	}
}
