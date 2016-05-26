package module

import (
	"time"

	"github.com/CSUNetSec/bgpmon/log"
)

const (
	COMDIE = iota
	COMRUN
	COMSTATUS
)

type ModuleCommand struct {
	Command int
	Args    []string
}

type Module struct {
	Moduler
	CommandChan  chan ModuleCommand
	runTicker    *time.Ticker
	timeoutTimer *time.Timer
}

type Moduler interface {
	Cleanup() error
	Run()     error
	Status()  string
}

func (m *Module) Init() error {
	m.CommandChan = make(chan ModuleCommand)
	go func() {
		commandChan := m.CommandChan
		for {
			//log.Debl.Printf("listening on command channel\n")
			select {
			case command := <-commandChan:
				switch command.Command {
				case COMDIE:
					//log.Debl.Printf("COMDIE for module:%+v\n", m)
					if m.runTicker != nil {
						m.runTicker.Stop()
					}
					if m.timeoutTimer != nil {
						m.timeoutTimer.Stop()
					}
					m.Cleanup()
					return
				case COMRUN:
					//log.Debl.Printf("COMRUN for module:%v\n", m)
					m.Run()
				case COMSTATUS:
					//log.Debl.Printf("COMSTATUS for module:%+v\n", m)
					m.Status()
				default:
					log.Errl.Printf("Command chan for module:%+v got undefined command number:%v", m, command)

				}
			}
		}
	}()

	return nil
}

func (m *Module) SchedulePeriodic(periodicSeconds, timeoutSeconds int32) error {
	runTicker := time.NewTicker(time.Duration(periodicSeconds) * time.Second)
	m.runTicker = runTicker
	runChan := runTicker.C

	timeoutTimer := time.NewTimer(time.Duration(timeoutSeconds) * time.Second)
	m.timeoutTimer = timeoutTimer
	timeoutChan := timeoutTimer.C

	go func() {
		for {
			select {
			case <-runChan:
				m.CommandChan <- ModuleCommand{COMRUN, nil}
			case <-timeoutChan:
				//TODO - what do we do here? shouldn't shut down the application, just stop it's execution
				//cchan <- ModuleCommand{COMDIE, nil}
			}
		}
	}()

	return nil
}
