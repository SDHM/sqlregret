package config

import (
	"encoding/json"
	"fmt"
	"time"
)

//过滤配置
type FilterConfig struct {
	FilterDb               string          // 过滤的数据库名称
	FilterTable            string          // 过滤的数据表名
	FilterSQL              string          // 过滤的语句类型(insert, update, delete) 默认为空表示三种都解析
	StartFileIndex         int             // 开始日志文件
	StartPos               int             // 日志解析起点
	startPosSet            bool            // 是否设置了解析文件起点
	endPosSet              bool            // 是否设置了解析文件终点
	StartTime              time.Time       // 日志解析开始时间点
	startTimeEnable        bool            // 开始时间是否设置了
	EndFileIndex           int             // 结束日志文件
	EndPos                 int             // 日志解析终点
	EndTime                time.Time       // 日志解析结束时间点
	endTimeEnable          bool            // 结束时间是否设置了
	Mode                   string          // 运行模式 parse:解析模式  mark:记录时间点模式
	NeedReverse            bool            // 是否需要反向操作语句
	WithDDL                bool            // 是否解析DDL语句 true:解析 false:不解析
	InsertFilterColumn     []*ColumnFilter // 插入操作列过滤器
	withInsertFilterColumn bool            // 是否有插入操作的列过滤器
	UpdateFilterColumn     []*ColumnFilter // 更新操作列过滤器
	withUpdateFilterColumn bool            // 是否有更新操作的列过滤器
	Dump                   bool            // 是否dump
	Origin                 bool            // 是否解析原始语句
	Limit                  int             // pre 模式下影响行数超过此值的予以显示
	Xid                    int64           // 单个事务解析
	BigTime                int             // 单个事务耗费时间过滤
}

type ColumnFilter struct {
	name   string // 列名
	before string // 修改前的值
	after  string // 修改后的值
}

func NewColumnFilter(name, before, after string) *ColumnFilter {
	this := new(ColumnFilter)
	this.name = name
	this.before = before
	this.after = after
	return this
}

func (this *ColumnFilter) GetName() string {
	return this.name
}

func (this *ColumnFilter) GetBefore() string {
	return this.before
}

func (this *ColumnFilter) GetAfter() string {
	return this.after
}

var (
	G_filterConfig = new(FilterConfig).Init()
)

func (this *FilterConfig) Init() FilterConfig {
	this.InsertFilterColumn = make([]*ColumnFilter, 0, 2)
	this.UpdateFilterColumn = make([]*ColumnFilter, 0, 2)
	return *this
}

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

func (this *FilterConfig) AppendInsertFilterColumn(filter *ColumnFilter) {
	this.InsertFilterColumn = append(this.InsertFilterColumn, filter)
	this.withInsertFilterColumn = true
}

func (this *FilterConfig) AppendUpdateFilterColumn(filter *ColumnFilter) {
	this.UpdateFilterColumn = append(this.UpdateFilterColumn, filter)
	this.withUpdateFilterColumn = true
}

func (this *FilterConfig) WithInsertFilterColumn() bool {
	return this.withInsertFilterColumn
}

func (this *FilterConfig) WithUpdateFilterColumn() bool {
	return this.withUpdateFilterColumn
}
