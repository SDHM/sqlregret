package instance

import (
	"github.com/SDHM/sqlregret/config"
)

type InstanceGenerator struct {
	cfg         *config.Config
	destInfoMap map[string]*config.InstanceConfig
}

func NewInstanceGenerator(cfg *config.Config) *InstanceGenerator {
	this := new(InstanceGenerator)
	this.cfg = cfg
	this.destInfoMap = make(map[string]*config.InstanceConfig, len(cfg.InstancesConfig))
	for index := range cfg.InstancesConfig {

		this.destInfoMap[cfg.InstancesConfig[index].Destination] = &config.InstanceConfig{
			Destination:       cfg.InstancesConfig[index].Destination,
			SlaveId:           cfg.InstancesConfig[index].SlaveId,
			MasterAddress:     cfg.InstancesConfig[index].MasterAddress,
			MasterPort:        cfg.InstancesConfig[index].MasterPort,
			MasterJournalName: cfg.InstancesConfig[index].MasterJournalName,
			MasterPosition:    cfg.InstancesConfig[index].MasterPosition,
			DbUsername:        cfg.InstancesConfig[index].DbUsername,
			DbPassword:        cfg.InstancesConfig[index].DbPassword,
			DefaultDbName:     cfg.InstancesConfig[index].DefaultDbName,
		}
	}
	return this
}

func (this *InstanceGenerator) Generate(destination string) *Instance {
	v, ok := this.destInfoMap[destination]
	if !ok {
		return nil
	} else {
		return NewInstance(v, this.cfg.InstanceLogPath, this.cfg.LogLevel)
	}

}
