package gobgp

import (
	"errors"
	"fmt"

	"github.com/hamersaw/bgpmon/module"
	"github.com/hamersaw/bgpmon/session"
)

type GoBGPLinkConfig struct {
}

func NewGoBGPLinkModule(address string, sessions []session.Session, config GoBGPLinkConfig) (module.Moduler, error) {
	fmt.Println("starting gobgp link module with arguments", address)
	return nil, errors.New("TODO - start gobgp module - create io sessions")
}
