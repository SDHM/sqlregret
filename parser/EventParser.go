package parser

import (
	"fmt"

	"github.com/SDHM/sqlregret/client"
	"github.com/SDHM/sqlregret/config"
	"github.com/SDHM/sqlregret/lifecycle"
)

type EventParser struct {
	runningMgr     *lifecycle.AbstractLifeCycle
	instCfg        *config.InstanceConfig
	connector      *client.MysqlConnection
	tableMetaCache *client.TableMetaCache
	destination    string
	slaveId        uint32
	masterPort     uint16
}

func NewEventParser(instCfg *config.InstanceConfig) *EventParser {

	this := new(EventParser)
	this.runningMgr = lifecycle.NewAbstractLifeCycle()
	this.instCfg = instCfg
	return this
}

func (this *EventParser) Start() error {

	this.runningMgr.Start()

	this.masterPort = uint16(this.instCfg.MasterPort)

	this.slaveId = uint32(this.instCfg.SlaveId)

	this.connector = client.NewMysqlConnection(
		this.instCfg.MasterAddress,
		this.instCfg.DbUsername,
		this.instCfg.DbPassword,
		this.instCfg.DefaultDbName,
		this.masterPort,
		this.slaveId)

	return this.Run()
}

func (this *EventParser) IsStart() bool {
	return this.runningMgr.IsStart()
}

func (this *EventParser) Stop() {
	this.runningMgr.Stop()
}

func (this *EventParser) Run() error {

	if this.instCfg.Mode == "online" {
		if err := this.connector.Connect(); nil != err {
			fmt.Println(err.Error())
			return err
		}

		if err := this.connector.Register(); nil != err {
			fmt.Println(err.Error())
			return err
		}

		if err := this.PreDump(); nil != err {
			return err
		}
	}

	if err := this.Dump(); nil != err {
		return nil
	}

	this.AfterDump()

	return nil
}

func (this *EventParser) PreDump() error {

	metaConnector := client.NewMysqlConnection(
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
	this.connector.SetTableMetaCache(this.tableMetaCache)
	return nil
}

func (this *EventParser) Dump() error {

	if err := this.connector.Dump(uint32(this.instCfg.MasterPosition),
		this.instCfg.MasterJournalName); nil != err {
		return err
	}

	return nil
}

func (this *EventParser) AfterDump() {
	this.tableMetaCache = nil
}
