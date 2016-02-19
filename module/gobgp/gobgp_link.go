package gobgp

import (
	"errors"
	"fmt"

	"github.com/hamersaw/bgpmon/module"
	"github.com/hamersaw/bgpmon/session"
)

func NewGoBGPLinkModule(address string, sessions []session.Session) (module.Moduler, error) {
	fmt.Println("starting gobgp link module with arguments", address)
	return nil, errors.New("TODO - start gobgp module - create io sessions")
}
