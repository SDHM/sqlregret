package instance

import (
	"fmt"
	"runtime"
	"sync"

	"sqlregret/config"
	"sqlregret/parser"
)

type Instance struct {
	instCfg     *config.Config
	Destination string
	EventParser *parser.EventParser
	lock        *sync.Mutex
}

func NewInstance(instCfg *config.Config) *Instance {

	this := new(Instance)
	this.Destination = instCfg.Destination
	this.instCfg = instCfg
	this.EventParser = parser.NewEventParser(this.instCfg)
	this.lock = &sync.Mutex{}
	return this
}

func (this *Instance) Close() {
	this.Stop()
}

func (this *Instance) Start() {

	defer func() {
		//if the error happend printf the current routine stacktrace
		if err := recover(); err != nil {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			fmt.Printf("Instance start gorouting panic %s: %v\n%s", this.Destination, err, buf)
		}
		this.Close()
	}()

	if !this.EventParser.IsStart() {
		if err := this.EventParser.Start(); nil != err {
			panic(err)
		}
	}
}

func (this *Instance) Stop() {
	if this.EventParser.IsStart() {
		this.EventParser.Stop()
	}
}

func (this *Instance) Lock() {
	this.lock.Lock()
}

func (this *Instance) UnLock() {
	this.lock.Unlock()
}
