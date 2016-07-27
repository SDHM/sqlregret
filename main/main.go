// tsg project main.go
package main

import (
	"flag"
	"fmt"
	"github.com/SDHM/sqlregret/config"
	"github.com/SDHM/sqlregret/server"
	"runtime"
)

var configFile *string = flag.String("config", "./tsg.conf", "tsg config file")
var logLevel *string = flag.String("log-level", "info", "log level [trace|debug|info|warn|error|fatal], default info")

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

	svr := server.NewServerController(cfg)

	if nil == svr {
		fmt.Println("svr new failed")
		return
	}

	err = svr.Init()
	if nil != err {
		fmt.Println(err.Error())
		return
	}

	svr.Start()
}
