package client

import (
	"fmt"
	"os"

	"time"

	"github.com/SDHM/sqlregret/config"
)

//收集事务消息
type Transaction struct {
	binlogFile string     // 当前事务所在的binlog文件
	endTime    *time.Time // 结束时间
	offset     int64      // 当前事务所在的偏移
	beginTime  *time.Time // 当前事务开始时间
	withBegin  bool       // 是否有事务开始标志
	withEnd    bool       // 是否有事务结束标志
	beSkip     bool       // 是否跳过了某些语句没解析
	sqlArray   []*ShowSql // sql语句数组
	sqlCount   int        // 事务事件总数
	xid        int64      // 事务id号
	outputFile *os.File
}

type ShowSql struct {
	bePrompt bool   // 只是提示
	sql      string // sql语句
	bePrint  bool   // 是否要打印
}

func NewShowSql(bePrompt bool, sql string, bePrint bool) *ShowSql {
	this := new(ShowSql)
	this.bePrint = bePrint
	this.bePrompt = bePrompt
	this.sql = sql
	return this
}

func (this *ShowSql) BePrompt() bool {
	return this.bePrompt
}

func (this *ShowSql) GetSql() string {
	return this.sql
}

func (this *ShowSql) BePrint() bool {
	return this.bePrint
}

var (
	G_transaction *Transaction
)

func NewTransaction(filename string) *Transaction {
	this := new(Transaction)
	if filename == "stdout" {
		this.outputFile = os.Stdout
	} else {
		if checkFileIsExist(filename) { //如果文件存在
			os.Remove(filename)
			this.outputFile, _ = os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666) //创建文件
		} else {
			this.outputFile, _ = os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666) //创建文件
		}
	}

	return this
}

func checkFileIsExist(filename string) bool {
	var exist = true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

func (this *Transaction) Begin(beginTime string, file string, pos int64) {
	this.binlogFile = file
	// this.beginTime = beginTime
	this.offset = pos
	this.withBegin = true
	this.withEnd = false
	this.xid = 1
	this.sqlCount = 0
	this.sqlArray = make([]*ShowSql, 0, 2)
}

func (this *Transaction) End(xid int64) {
	this.withEnd = true
	this.xid = xid
}

func (this *Transaction) AppendSQL(t *time.Time, sql *ShowSql) {

	if config.G_filterConfig.Mode == "bigt" {
		if this.beginTime == nil {
			this.beginTime = t
		}
		if this.endTime == nil {
			this.endTime = t
		} else {
			if (*t).After(*this.endTime) {
				this.endTime = t
			}
		}
	}

	if config.G_filterConfig.Mode == "pre" {
		this.appendCount()
		return
	}

	this.sqlArray = append(this.sqlArray, sql)
}

func (this *Transaction) appendCount() {
	this.sqlCount++
}

func (this *Transaction) PrintTransaction() {
	this.output(this.withBegin && this.withEnd)
	this.withBegin = false
	this.beSkip = false
}

func (this *Transaction) IsTransactionEnd() bool {
	return this.withEnd && !this.withBegin
}

//跳过了一些语句未解析
func (this *Transaction) SkipSomeThing() {
	this.beSkip = true
}

func (this *Transaction) output(full bool) {
	if config.G_filterConfig.Xid == 0 {
		this.oneTransactionOutPut(full)
	} else {
		if config.G_filterConfig.Xid == this.xid {
			this.oneTransactionOutPut(full)
			this.outputFile.WriteString("事务解析完毕\n")
			os.Exit(1)
		}
	}
}

func (this *Transaction) oneTransactionOutPut(full bool) {

	second := (*this.endTime).Sub(*this.beginTime).Seconds()
	if config.G_filterConfig.Mode == "bigt" && int(second) < config.G_filterConfig.BigTime {
		this.beginTime = nil
		this.endTime = nil
		this.sqlArray = nil
		return
	}

	effectorRow := this.sqlCount / 4
	if config.G_filterConfig.Mode == "pre" && effectorRow > config.G_filterConfig.Limit {
		str := fmt.Sprintf("事务文件:%s\t事务偏移:%d\t事务影响行数:%d\t事务ID:%d\n", this.binlogFile, this.offset, effectorRow, this.xid)
		this.outputFile.WriteString(str)
		return
	}

	if len(this.sqlArray) > 0 && !config.G_filterConfig.Dump {
		str := fmt.Sprintf("\n事务开始\n")
		this.WriteAll(str)
	}

	if !full {
		if !config.G_filterConfig.Dump {
			str := fmt.Sprintf("这是一个不完整的事务\n")
			this.WriteAll(str)
		}
	}

	if full && this.beSkip && len(this.sqlArray) >= 1 {
		if !config.G_filterConfig.Dump {
			str := fmt.Sprintln("这是一个不完整的事务")
			this.WriteAll(str)
		}
	}

	for _, sql := range this.sqlArray {
		if sql.BePrint() {
			str := fmt.Sprintf(sql.GetSql())
			this.WriteAll(str)
		}
	}

	if len(this.sqlArray) > 0 && !config.G_filterConfig.Dump {
		str := fmt.Sprintf("提交事务:%d", this.xid)

		if config.G_filterConfig.Mode == "bigt" {
			second := (*this.endTime).Sub(*this.beginTime).Seconds()

			if second >= float64(config.G_filterConfig.BigTime) {
				str += fmt.Sprintf("\t时间跨度:%.2f", second)
			}
		}

		str += "\n\n"
		this.WriteAll(str)
	}

	this.sqlArray = nil
	this.beginTime = nil
	this.endTime = nil
}

func (this *Transaction) WriteAll(str string) {
	if nil == this.outputFile {
		return
	}

	length := len(str)
	writeLen := 0
	for writeLen != length {
		if n, err := this.outputFile.WriteString(str[writeLen:]); nil != err {
			return
		} else {
			writeLen += n
		}
	}
}
