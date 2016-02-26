package module

import (
	"errors"
	"time"

	"github.com/CSUNetSec/bgpmon/log"
)

const (
	COMDIE = iota
	COMRUN
	COMSTATUS
	NUMCOM
)

var (
	comstrs = [NUMCOM]string{
		"DIE",
		"RUN",
		"STATUS",
	}
)

type Module struct {
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

func NewModuleCommand(command string, args []string) (*ModuleCommand, error) {
	for i, v := range comstrs {
		if command == v {
			return &ModuleCommand{Command: i, Args: args}, nil
		}
	}
	return nil, errors.New("no such command supported by the module infrastructure")
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
					log.Debl.Printf("COMRUN for module:%+v\n", m)
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

func SchedulePeriodic(m Moduler, periodicSeconds, timeoutSeconds uint32) error {
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
				log.Debl.Printf("Running module:%+v\n", m)
				command, _ := NewModuleCommand("COMRUN", nil)
				cchan <- *command
			case <-tchan:
				log.Debl.Printf("Timeout for module:%+v\n", m)
				command, _ := NewModuleCommand("COMDIE", nil)
				cchan <- *command
			}
		}
	}(m)

	return nil
}
