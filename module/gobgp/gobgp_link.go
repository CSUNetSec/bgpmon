package gobgp

import (
	"errors"
	"fmt"

	"github.com/CSUNetSec/bgpmon/module"
	"github.com/CSUNetSec/bgpmon/session"
)

type GoBGPLinkConfig struct {
}

func NewGoBGPLinkModule(address string, sessions []session.Session, config GoBGPLinkConfig) (*module.Module, error) {
	fmt.Println("starting gobgp link module with arguments", address)
	return nil, errors.New("TODO - start gobgp module - create io sessions")
}
