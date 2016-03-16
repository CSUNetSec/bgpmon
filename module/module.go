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

/*type Module struct {
	commandChannel chan ModuleCommand
	runTicker      *time.Ticker
	timeoutTimer   *time.Timer
}

type Moduler interface {
	GetCommandChannel() chan ModuleCommand
	Run() error
	Status() string
	Cleanup()
	SetRunTicker(*time.Ticker)
	GetRunTicker() *time.Ticker
	SetTimeoutTimer(*time.Timer)
	GetTimeoutTimer() *time.Timer
}

type ModuleCommand struct {
	Command int
	Args    []string
}

func NewModule() Module {
	return Module{commandChannel: make(chan ModuleCommand)}
}

func (m *Module) GetCommandChannel() chan ModuleCommand {
	return m.commandChannel
}

func (m *Module) SetRunTicker(runTicker *time.Ticker) {
	m.runTicker = runTicker
}

func (m *Module) GetRunTicker() *time.Ticker {
	return m.runTicker
}

func (m *Module) SetTimeoutTimer(timeoutTimer *time.Timer) {
	m.timeoutTimer = timeoutTimer
}

func (m *Module) GetTimeoutTimer() *time.Timer {
	return m.timeoutTimer
}

func Init(m Moduler) error {
	go func(m Moduler) {
		cchan := m.GetCommandChannel()
		for {
			select {
			case command := <-cchan:
				switch command.Command {
				case COMDIE:
					log.Debl.Printf("COMDIE for module:%+v\n", m)
					if m.GetRunTicker() != nil {
						m.GetRunTicker().Stop()
					}
					if m.GetTimeoutTimer != nil {
						m.GetTimeoutTimer().Stop()
					}
					m.Cleanup()
					return
				case COMRUN:
					log.Debl.Printf("COMRUN for module:%v\n", m)
					m.Run()
				case COMSTATUS:
					log.Debl.Printf("COMSTATUS for module:%+v\n", m)
					m.Status()
				default:
					log.Errl.Printf("Command chan for module:%+v got undefined command number:%v", m, command)

				}
			}
		}
	}(m)

	return nil
}

func SchedulePeriodic(m Moduler, periodicSeconds, timeoutSeconds int32) error {
	runTicker := time.NewTicker(time.Duration(periodicSeconds) * time.Second)
	m.SetRunTicker(runTicker)
	pchan := runTicker.C

	timeoutTimer := time.NewTimer(time.Duration(timeoutSeconds) * time.Second)
	m.SetTimeoutTimer(timeoutTimer)
	tchan := timeoutTimer.C

	go func(m Moduler) {
		cchan := m.GetCommandChannel()
		for {
			select {
			case <-pchan:
				cchan <- ModuleCommand{COMRUN, nil}
			case <-tchan:
				//TODO - what do we do here? shouldn't shut down the application, just stop it's execution
				//cchan <- ModuleCommand{COMDIE, nil}
			}
		}
	}(m)

	return nil
}*/

//new code
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

func (m Module) Init() error {
	go func() {
		commandChan := m.CommandChan
		for {
			select {
			case command := <-commandChan:
				switch command.Command {
				case COMDIE:
					log.Debl.Printf("COMDIE for module:%+v\n", m)
					if m.runTicker != nil {
						m.runTicker.Stop()
					}
					if m.timeoutTimer != nil {
						m.timeoutTimer.Stop()
					}
					m.Cleanup()
					return
				case COMRUN:
					log.Debl.Printf("COMRUN for module:%v\n", m)
					m.Run()
				case COMSTATUS:
					log.Debl.Printf("COMSTATUS for module:%+v\n", m)
					m.Status()
				default:
					log.Errl.Printf("Command chan for module:%+v got undefined command number:%v", m, command)

				}
			}
		}
	}()

	return nil
}

func (m Module) SchedulePeriodic(periodicSeconds, timeoutSeconds int32) error {
	runTicker := time.NewTicker(time.Duration(periodicSeconds) * time.Second)
	m.runTicker = runTicker
	runChan := runTicker.C

	timeoutTimer := time.NewTimer(time.Duration(timeoutSeconds) * time.Second)
	m.timeoutTimer = timeoutTimer
	timeoutChan := timeoutTimer.C

	go func() {
		commandChan := m.CommandChan
		for {
			select {
			case <-runChan:
				commandChan <- ModuleCommand{COMRUN, nil}
			case <-timeoutChan:
				//TODO - what do we do here? shouldn't shut down the application, just stop it's execution
				//cchan <- ModuleCommand{COMDIE, nil}
			}
		}
	}()

	return nil
}
