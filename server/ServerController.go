package server

import (
	"errors"
	"os"
	"strings"
	"time"

	"github.com/SDHM/sqlregret/config"
	"github.com/siddontang/go-log/log"
)

type ServerController struct {
	cfg             *config.Config
	logger          *log.Logger
	parserContainer *ParserContainer
}

func setLogLevel(logger *log.Logger, level string) {
	switch strings.ToLower(level) {
	case "trace":
		logger.SetLevel(log.LevelTrace)
	case "debug":
		logger.SetLevel(log.LevelDebug)
	case "info":
		logger.SetLevel(log.LevelInfo)
	case "warn":
		logger.SetLevel(log.LevelWarn)
	case "error":
		logger.SetLevel(log.LevelError)
	case "fatal":
		logger.SetLevel(log.LevelFatal)
	default:
		logger.SetLevel(log.LevelInfo)
	}
}

func NewServerController(cfg *config.Config) *ServerController {
	this := new(ServerController)
	this.cfg = cfg

	os.Mkdir(cfg.ServerLogPath, 0777)

	if h, err := log.NewRotatingFileHandler(cfg.ServerLogPath+"/serverlog.log", 1024*1024*5, 2); nil != err {
		this.logger = nil
	} else {
		this.logger = log.NewDefault(h)
		setLogLevel(this.logger, this.cfg.LogLevel)
	}

	return this
}

func (this *ServerController) Init() error {

	this.parserContainer = NewParserContainer(this.cfg, this.logger)

	if nil == this.parserContainer {
		errs := errors.New("embeded server init failed")
		this.logger.Error(errs.Error())
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
