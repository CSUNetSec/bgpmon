package gobgp

import (
	"errors"

	"github.com/hamersaw/bgpmon/module"
	"github.com/hamersaw/bgpmon/session"
)

func NewGoBGPLinkModule(configStr string, sessions session.IOSessions) (module.Moduler, error) {
	return nil, errors.New("TODO - start gobgp module - create io sessions")
}
