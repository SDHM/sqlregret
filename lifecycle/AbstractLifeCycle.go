package lifecycle

import (
	"sync"
)

type AbstractLifeCycle struct {
	running      bool
	runningMutex *sync.Mutex
}

func NewAbstractLifeCycle() *AbstractLifeCycle {
	this := new(AbstractLifeCycle)
	this.running = false
	this.runningMutex = &sync.Mutex{}
	return this
}

func (this *AbstractLifeCycle) IsStart() bool {

	this.runningMutex.Lock()
	defer this.runningMutex.Unlock()

	return this.running
}

func (this *AbstractLifeCycle) Start() {

	if this.running {
		return
	}

	this.runningMutex.Lock()
	defer this.runningMutex.Unlock()
	this.running = true
}

func (this *AbstractLifeCycle) Stop() {

	if !this.running {
		return
	}

	this.runningMutex.Lock()
	defer this.runningMutex.Unlock()
	this.running = false
}

func (this *AbstractLifeCycle) IsRunning() bool {
	return this.running
}
