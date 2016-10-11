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
	StartFileIndex  int       // 开始日志文件
	StartPos        int       // 日志解析起点
	startPosSet     bool      // 是否设置了解析文件起点
	endPosSet       bool      // 是否设置了解析文件终点
	StartTime       time.Time // 日志解析开始时间点
	startTimeEnable bool      // 开始时间是否设置了
	EndFileIndex    int       // 结束日志文件
	EndPos          int       // 日志解析终点
	EndTime         time.Time // 日志解析结束时间点
	endTimeEnable   bool      // 结束时间是否设置了
	Mode            string    // 运行模式 parse:解析模式  mark:记录时间点模式
	NeedReverse     bool      // 是否需要反向操作语句
	WithDDL         bool      // 是否解析DDL语句 true:解析 false:不解析
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

func (this *FilterConfig) SetStartPos(index int, pos int) {
	this.startPosSet = true
	this.StartFileIndex = index
	this.StartPos = pos
}

func (this *FilterConfig) SetEndPos(index int, pos int) {
	this.endPosSet = true
	this.EndFileIndex = index
	this.EndPos = pos
}

func (this *FilterConfig) StartPosEnable() bool {
	return this.startPosSet
}

func (this *FilterConfig) EndPosEnable() bool {
	return this.endPosSet
}

func (this *FilterConfig) StartTimeEnable() bool {
	return this.startTimeEnable
}

func (this *FilterConfig) EndTimeEnable() bool {
	return this.endTimeEnable
}
