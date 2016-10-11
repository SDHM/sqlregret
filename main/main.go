// tsg project main.go
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/SDHM/sqlregret/config"
	"github.com/SDHM/sqlregret/instance"
	"github.com/cihub/seelog"
)

var (
	configFile  *string = flag.String("config", "./sqlregret.conf", "后悔药配置文件")
	logcfgFile          = flag.String("logcfg", "./seelog.xml", "日志配置文件")
	help                = flag.String("help", "help", "帮助文档")
	filterDb            = flag.String("filter-db", "", "过滤的数据库名称")
	filterTable         = flag.String("filter-table", "", "过滤的数据表名")
	filterSQL           = flag.String("filter-sql", "", "过滤的语句类型(insert, update, delete) 默认为空表示三种都解析")
	startFile           = flag.String("start-file", "", "开始日志文件")
	endFile             = flag.String("end-file", "", "结束日志文件")
	startPos            = flag.Int("start-pos", 0, "日志解析起点")
	endPos              = flag.Int("end-pos", 0, "日志解析终点")
	startTime           = flag.String("start-time", "", "日志解析开始时间点")
	endTime             = flag.String("end-time", "", "日志解析结束时间点")
	mode                = flag.String("mode", "mark", "运行模式 parse:解析模式  mark:记录时间点模式")
	needReverse         = flag.Bool("rsv", true, "是否需要反向操作语句")
	withDDL             = flag.Bool("with-ddl", false, "是否解析ddl语句")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()
	ConfigCheck()

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

func ConfigCheck() {

	//打印帮助
	if *help == "" {
		flag.Usage()
		os.Exit(1)
	}

	if len(*configFile) == 0 {
		fmt.Println("配置文件不准为空")
		flag.Usage()
		os.Exit(1)
	}

	config.G_filterConfig.Mode = strings.ToLower(*mode)
	if config.G_filterConfig.Mode != "mark" && config.G_filterConfig.Mode != "parse" {
		fmt.Println("mode必须为mark或者是parse")
		flag.Usage()
		os.Exit(1)
	}

	config.G_filterConfig.NeedReverse = *needReverse
	config.G_filterConfig.FilterSQL = strings.ToLower(*filterSQL)
	if config.G_filterConfig.FilterSQL != "update" &&
		config.G_filterConfig.FilterSQL != "delete" &&
		config.G_filterConfig.FilterSQL != "insert" &&
		config.G_filterConfig.FilterSQL != "" {
		fmt.Println("filter-sql必须为insert、update、delete或者为空")
		flag.Usage()
		os.Exit(1)
	}

	config.G_filterConfig.WithDDL = *withDDL
	//检查开始时间与结束时间

	if *startTime == "" && *endTime != "" {
		fmt.Println("不允许不存在开始时间却有结束时间的情况")
		os.Exit(1)
	}

	if *startTime != "" {
		if t, err := time.ParseInLocation("2006-01-02 15:04:05", *startTime, time.Local); nil != err {
			fmt.Println("请检查您的开始时间")
			os.Exit(1)
		} else {
			config.G_filterConfig.SetStartTime(t)
		}
	}

	if *endTime != "" {
		if t, err := time.ParseInLocation("2006-01-02 15:04:05", *endTime, time.Local); nil != err {
			fmt.Println("请检查您的结束时间")
			os.Exit(1)
		} else {
			config.G_filterConfig.SetEndTime(t)
		}
	}

	//检查过滤的数据库与数据库表名
	if *filterTable != "" && *filterDb == "" {
		fmt.Println("请指定要过滤的数据库名称")
		os.Exit(1)
	} else {
		config.G_filterConfig.FilterTable = *filterTable
		config.G_filterConfig.FilterDb = *filterDb
	}

	//检查开始时间与开始位置
	if *startFile != "" && *startPos != 0 {
		config.G_filterConfig.StartFile = *startFile
		config.G_filterConfig.StartPos = *startPos
	} else if *startFile == "" && *startPos == 0 {

	} else {
		fmt.Println("开始文件与开始位置必须同时设置值")
		os.Exit(1)
	}

	//检查结束文件与结束位置
	if *endFile != "" && *endPos != 0 {
		config.G_filterConfig.EndFile = *endFile
		config.G_filterConfig.EndPos = *endPos
	} else if *endFile == "" && *endPos == 0 {

	} else {
		fmt.Println("结束文件与结束位置必须同时设置值")
		os.Exit(1)
	}
}
