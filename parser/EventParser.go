package parser

import (
	"errors"

	"github.com/SDHM/sqlregret/client"
	"github.com/SDHM/sqlregret/config"
	"github.com/SDHM/sqlregret/lifecycle"
	"github.com/cihub/seelog"
)

type EventParser struct {
	runningMgr     *lifecycle.AbstractLifeCycle
	instCfg        *config.Config
	reader         client.IBinlogReader
	tableMetaCache *client.TableMetaCache
	destination    string
	slaveId        uint32
	masterPort     uint16
}

func NewEventParser(instCfg *config.Config) *EventParser {

	this := new(EventParser)
	this.runningMgr = lifecycle.NewAbstractLifeCycle()
	this.instCfg = instCfg
	return this
}

func (this *EventParser) Start() error {

	this.runningMgr.Start()

	this.masterPort = uint16(this.instCfg.MasterPort)

	this.slaveId = uint32(this.instCfg.SlaveId)

	if this.instCfg.Mode == "online" {
		this.reader = client.NewNetBinlogReader(
			this.instCfg.MasterAddress,
			this.instCfg.DbUsername,
			this.instCfg.DbPassword,
			this.instCfg.DefaultDbName,
			this.masterPort,
			this.slaveId)
	} else if this.instCfg.Mode == "onfile" {
		this.reader = client.NewFileBinlogReader(
			this.instCfg.DefaultDbName,
			this.instCfg.IndexFile,
			this.instCfg.BasePath)
	} else {
		seelog.Errorf("暂时不支持这种类型:%s", this.instCfg.Mode)
		return errors.New("不支持这种方式")
	}

	return this.Run()
}

func (this *EventParser) IsStart() bool {
	return this.runningMgr.IsStart()
}

func (this *EventParser) Stop() {
	this.runningMgr.Stop()
}

func (this *EventParser) Run() error {

	if err := this.reader.Connect(); nil != err {
		return err
	}

	if err := this.reader.Register(); nil != err {
		return err
	}

	if err := this.PreDump(); nil != err {
		return err
	}

	if err := this.reader.Dump(uint32(this.instCfg.MasterPosition),
		this.instCfg.MasterJournalName); nil != err {
		return err
	}

	this.AfterDump()

	return nil
}

func (this *EventParser) PreDump() error {

	metaConnector := client.NewNetBinlogReader(
		this.instCfg.MasterAddress,
		this.instCfg.DbUsername,
		this.instCfg.DbPassword,
		this.instCfg.DefaultDbName,
		this.masterPort,
		this.slaveId)

	if err := metaConnector.Connect(); nil != err {
		return err
	}

	this.tableMetaCache = client.NewTableMetaCache(metaConnector)
	this.reader.SetTableMetaCache(this.tableMetaCache)
	return nil
}

func (this *EventParser) AfterDump() {
	this.tableMetaCache = nil
}
