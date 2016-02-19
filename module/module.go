package module

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/hamersaw/bgpmon/log"
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
	Configstr string
	Timers    TimerConfig
	comch     chan ModuleCommand
	Id        string
}

type ModuleCommand struct {
	command int
	args    []string
	//XXX: for more complicated commands we can add a channel for the result to be returned.
}

func MakeCommand(command string, args []string) (*ModuleCommand, error) {
	for i, v := range comstrs {
		if command == v {
			return &ModuleCommand{command: i, args: args}, nil
		}
	}
	return nil, errors.New("no such command supported by the module infrastructure")
}

type TimerConfig struct {
	TimeoutSecs  int `json:"TimeoutSeconds"`
	PeriodicSecs int `json:"PeriodicSeconds"`
}

type Moduler interface {
	GetComChan() chan ModuleCommand
	Run() error
	Status() string
	Cleanup()
	getTimers() (int, int)
	getId() string
	SetId(string)
}

func (m *Module) ParseTimerConfig(conf string) error {
	if err := json.Unmarshal([]byte(conf), &m.Timers); err != nil {
		return err
	}
	return nil
}

func (m *Module) GetComChan() chan ModuleCommand {
	return m.comch
}

func (m *Module) getId() string {
	return m.Id
}

func (m *Module) SetId(a string) {
	m.Id = a
}

//returns the periodic and timeout settings as integers
func (m *Module) getTimers() (int, int) {
	return m.Timers.PeriodicSecs, m.Timers.TimeoutSecs
}

func NewModule(conf string) Module {
	return Module{Configstr: conf, comch: make(chan ModuleCommand)}
}

func Init(a Moduler, rmap *map[string]Moduler) error {
	var (
		pchan, tchan <-chan time.Time //this chans have not been initialized yet so select should ignore them
		tick         *time.Ticker
		timer        *time.Timer
	)
	psec, tsec := a.getTimers()
	if psec != 0 {
		tick = time.NewTicker(time.Duration(psec) * time.Second)
		pchan = tick.C //now pchan is not nil
	}
	if tsec != 0 {
		//although quite extreme, sometimes we might get a COMDIE
		//while the timer goroutine is running, and this might get us eventually bombed.
		//keep a ref to the timeout timer to stop it explicitely.
		timer = time.NewTimer(time.Duration(tsec) * time.Second)
		tchan = timer.C
	}
	(*rmap)[a.getId()] = a //safe the module under an id
	log.Debl.Printf("Module:%+v, starting under ID:%s\n", a, a.getId())
	go func(a Moduler) {
		cchan := a.GetComChan()
		for {
			select {
			case command := <-cchan:
				switch command.command {
				case COMDIE:
					log.Debl.Printf("COMDIE for module:%+v\n", a)
					if tick != nil {
						tick.Stop()
					}
					if timer != nil {
						timer.Stop()
					}
					delete(*rmap, a.getId())
					log.Debl.Printf("running map of server that called the module:%+v", *rmap)
					a.Cleanup()
					return
				case COMRUN:
					log.Debl.Printf("COMRUN for module:%+v\n", a)
					a.Run()
				case COMSTATUS:
					log.Debl.Printf("COMSTATUS for module:%+v\n", a)
					a.Status()
				default:
					log.Errl.Printf("Command chan for module:%+v got undefined command number:%v", a, command)

				}
			case <-pchan:
				log.Debl.Printf("Running module:%+v\n", a)
				a.Run()
			case <-tchan:
				log.Debl.Printf("Timeout for module:%+v\n", a)
				if tick != nil {
					tick.Stop()
				}
				delete(*rmap, a.getId())
				log.Debl.Printf("running map of server that called the module:%+v", *rmap)
				a.Cleanup()
				return
			}
		}
	}(a)
	return nil
}
