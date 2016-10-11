package config

import (
	"encoding/json"
	"fmt"
	"time"
)

//过滤配置
type FilterConfig struct {
	FilterDb        string    // 过滤的数据库名称
	FilterTable     string    // 过滤的数据表名
	FilterSQL       string    // 过滤的语句类型(insert, update, delete) 默认为空表示三种都解析
	StartFile       string    // 开始日志文件
	StartPos        int       // 日志解析起点
	StartTime       time.Time // 日志解析开始时间点
	startTimeEnable bool      // 开始时间是否设置了
	EndFile         string    // 结束日志文件
	EndPos          int       // 日志解析终点
	EndTime         time.Time // 日志解析结束时间点
	endTimeEnable   bool      // 结束时间是否设置了
	Mode            string    // 运行模式 parse:解析模式  mark:记录时间点模式
	NeedReverse     bool      // 是否需要反向操作语句
}

var (
	G_filterConfig FilterConfig
)

func (this *FilterConfig) JsonEncoder() string {
	if buf, err := json.Marshal(this); nil != err {
		return ""
	} else {
		return string(buf)
	}
}

func (this *FilterConfig) Print() {
	fmt.Println(this.JsonEncoder())
}

func (this *FilterConfig) SetStartTime(t time.Time) {
	this.startTimeEnable = true
	this.StartTime = t
}

func (this *FilterConfig) SetEndTime(t time.Time) {
	this.endTimeEnable = true
	this.EndTime = t
}

func (this *FilterConfig) StartTimeEnable() bool {
	return this.startTimeEnable
}

func (this *FilterConfig) EndTimeEnable() bool {
	return this.endTimeEnable
}
