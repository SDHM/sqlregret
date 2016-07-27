package instance

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/SDHM/sqlregret/config"
	"github.com/SDHM/sqlregret/lifecycle"
	"github.com/SDHM/sqlregret/parser"
	"github.com/siddontang/go-log/log"
)

type Instance struct {
	runningManager *lifecycle.AbstractLifeCycle
	instCfg        *config.InstanceConfig
	Destination    string
	EventParser    *parser.EventParser
	logger         *log.Logger
	lock           *sync.Mutex
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

func NewInstance(instCfg *config.InstanceConfig, logpath string, loglevel string) *Instance {

	this := new(Instance)
	this.runningManager = lifecycle.NewAbstractLifeCycle()
	this.Destination = instCfg.Destination
	this.instCfg = instCfg

	os.Mkdir(logpath, 0777)
	if h, err := log.NewRotatingFileHandler(logpath+"/"+this.Destination+".log", 1024*1024*10, 3); nil != err {
		this.logger = nil
	} else {
		this.logger = log.NewDefault(h)
		setLogLevel(this.logger, loglevel)
	}

	this.EventParser = parser.NewEventParser(this.instCfg, this.logger)
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

	this.runningManager.Start()

	if !this.EventParser.IsStart() {
		if err := this.EventParser.Start(); nil != err {
			panic(err)
		}
	}
}

func (this *Instance) IsStart() bool {
	return this.runningManager.IsStart()
}
func (this *Instance) Stop() {

	if this.EventParser.IsStart() {
		this.EventParser.Stop()
	}

	this.runningManager.Stop()
}

func (this *Instance) Lock() {
	this.lock.Lock()
}

func (this *Instance) UnLock() {
	this.lock.Unlock()
}
