package server

import (
	"github.com/SDHM/sqlregret/config"
	"github.com/SDHM/sqlregret/instance"
	"github.com/SDHM/sqlregret/lifecycle"
)

type ParserContainer struct {
	runningMgr        *lifecycle.AbstractLifeCycle
	cfg               *config.Config
	instances         map[string]*instance.Instance
	instanceGenerator *instance.InstanceGenerator
}

func NewParserContainer(cfg *config.Config) *ParserContainer {
	this := new(ParserContainer)
	this.cfg = cfg
	this.runningMgr = lifecycle.NewAbstractLifeCycle()
	this.instances = make(map[string]*instance.Instance, len(cfg.InstancesConfig))
	this.instanceGenerator = instance.NewInstanceGenerator(cfg)
	return this
}

func (this *ParserContainer) Start() {
	this.runningMgr.Start()
	this.InitInstances()
}

func (this *ParserContainer) IsStart() bool {
	return this.runningMgr.IsStart()
}

func (this *ParserContainer) Stop() {
	this.runningMgr.Stop()

	for _, v := range this.instances {
		if v.IsStart() {
			v.Stop()
		}
	}
}

func (this *ParserContainer) InitInstances() error {
	for index := range this.cfg.InstancesConfig {
		destention := this.cfg.InstancesConfig[index].Destination
		inst := this.instanceGenerator.Generate(destention)
		this.instances[destention] = inst
		go inst.Start()
	}

	return nil
}

func (this *ParserContainer) GetInstance(destination string) *instance.Instance {

	v, ok := this.instances[destination]
	if !ok {
		v = this.instanceGenerator.Generate(destination)
	}
	return v
}

func (this *ParserContainer) StartWithDestination(destention string) {
	inst := this.GetInstance(destention)

	if !inst.IsStart() {
		inst.Start()
	}
}

func (this *ParserContainer) StopWithDestination(destination string) {
	inst := this.GetInstance(destination)

	if inst.IsStart() {
		inst.Stop()
	}
}

func (this *ParserContainer) IsStartWithDestination(destention string) bool {
	v, ok := this.instances[destention]
	if ok && v.IsStart() {
		return true
	}
	return false
}
