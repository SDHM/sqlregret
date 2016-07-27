package parser

import (
	"fmt"
	"strconv"

	"github.com/SDHM/sqlregret/client"
	"github.com/SDHM/sqlregret/config"
	"github.com/SDHM/sqlregret/lifecycle"
	"github.com/siddontang/go-log/log"
)

type EventParser struct {
	runningMgr     *lifecycle.AbstractLifeCycle
	instCfg        *config.InstanceConfig
	logger         *log.Logger
	connector      *client.MysqlConnection
	tableMetaCache *client.TableMetaCache
	destination    string
	slaveId        uint32
	masterPort     uint16
}

func NewEventParser(
	instCfg *config.InstanceConfig,
	logger *log.Logger) *EventParser {

	this := new(EventParser)
	this.runningMgr = lifecycle.NewAbstractLifeCycle()
	this.instCfg = instCfg
	this.logger = logger
	return this
}

func (this *EventParser) Start() error {

	this.runningMgr.Start()

	if port, err := strconv.Atoi(this.instCfg.MasterPort); nil != err {
		return err
	} else {
		this.masterPort = uint16(port)
	}

	if slaveId, err := strconv.Atoi(this.instCfg.SlaveId); nil != err {
		return err
	} else {
		this.slaveId = uint32(slaveId)
	}

	this.connector = client.NewMysqlConnection(
		this.instCfg.MasterAddress,
		this.instCfg.DbUsername,
		this.instCfg.DbPassword,
		this.instCfg.DefaultDbName,
		this.masterPort,
		this.slaveId,
		this.logger)

	return this.Run()
}

func (this *EventParser) IsStart() bool {
	return this.runningMgr.IsStart()
}

func (this *EventParser) Stop() {
	this.runningMgr.Stop()
}

func (this *EventParser) Run() error {
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
		this.slaveId,
		this.logger)

	if err := metaConnector.Connect(); nil != err {
		return err
	}

	this.tableMetaCache = client.NewTableMetaCache(metaConnector)
	this.connector.SetTableMetaCache(this.tableMetaCache)
	return nil
}

func (this *EventParser) Dump() error {

	logPos, err := strconv.Atoi(this.instCfg.MasterPosition)
	if nil != err {
		return err
	}

	if err := this.connector.Dump(uint32(logPos), this.instCfg.MasterJournalName); nil != err {
		return err
	}

	return nil
}
func (this *EventParser) AfterDump() {
	this.tableMetaCache = nil
}
