package server

import (
	"errors"
	"log"
	"time"

	"github.com/SDHM/sqlregret/config"
	"github.com/cihub/seelog"
)

type ServerController struct {
	cfg             *config.Config
	logger          *log.Logger
	parserContainer *ParserContainer
}

func NewServerController(cfg *config.Config) *ServerController {
	this := new(ServerController)
	this.cfg = cfg
	return this
}

func (this *ServerController) Init() error {

	this.parserContainer = NewParserContainer(this.cfg)

	if nil == this.parserContainer {
		errs := errors.New("embeded server init failed")
		seelog.Error(errs.Error())
		return errs
	}

	return nil
}

func (this *ServerController) Start() {
	this.parserContainer.Start()

	// 保证不退出
	for {
		time.Sleep(time.Millisecond * 500)
	}
}
