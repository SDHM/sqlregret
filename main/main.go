// tsg project main.go
package main

import (
	"flag"
	"fmt"
	"runtime"

	"github.com/SDHM/sqlregret/config"
	"github.com/SDHM/sqlregret/instance"
	"github.com/cihub/seelog"
)

var (
	configFile *string = flag.String("config", "./sqlregret.conf", "sqlregret config file")
	logcfgFile         = flag.String("logcfg", "./seelog.xml", "log config file")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()
	if len(*configFile) == 0 {
		fmt.Println("must use a config file")
		return
	}

	cfg, err := config.ParseConfigFile(*configFile)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// 初始化log
	defer seelog.Flush()
	if err := initLogger(*logcfgFile); err != nil {
		seelog.Debug("initLogger failed, log config path:", *logcfgFile, " err:", err)
		return
	}

	instance := instance.NewInstance(cfg)

	if nil == instance {
		fmt.Println("svr new failed")
		return
	}

	instance.Start()
}

func initLogger(cfgPath string) error {
	logger, err := seelog.LoggerFromConfigAsFile(cfgPath)
	if err != nil {
		return err
	}
	seelog.ReplaceLogger(logger)
	return nil
}
