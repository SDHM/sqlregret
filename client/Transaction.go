package client

import (
	"fmt"

	"github.com/SDHM/sqlregret/config"
)

//收集事务消息
type Transaction struct {
	withBegin bool       // 是否有事务开始标志
	withEnd   bool       // 是否有事务结束标志
	beSkip    bool       // 是否跳过了某些语句没解析
	sqlArray  []*ShowSql // sql语句数组
	xid       int64      // 事务id号
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
	return this.bePrint
}

func (this *ShowSql) GetSql() string {
	return this.sql
}

func (this *ShowSql) BePrint() bool {
	return this.bePrint
}

var (
	g_transaction Transaction
)

func (this *Transaction) Begin() {
	this.withBegin = true
	this.withEnd = false
	this.xid = 1
	this.sqlArray = make([]*ShowSql, 0, 2)
}

func (this *Transaction) End(xid int64) {
	this.withEnd = true
	this.xid = xid
}

func (this *Transaction) AppendSQL(sql *ShowSql) {
	this.sqlArray = append(this.sqlArray, sql)
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
	if len(this.sqlArray) > 0 && !config.G_filterConfig.Dump {
		fmt.Println("\n事务开始")
	}

	if !full {
		if !config.G_filterConfig.Dump {
			fmt.Println("这是一个不完整的事务")
		}
	}

	if full && this.beSkip && len(this.sqlArray) >= 1 {
		if !config.G_filterConfig.Dump {
			fmt.Println("这是一个不完整的事务")
		}
	}

	for _, sql := range this.sqlArray {
		if sql.BePrint() {
			fmt.Print(sql.GetSql())
		}
	}

	if len(this.sqlArray) > 0 && !config.G_filterConfig.Dump {
		fmt.Printf("提交事务:%d\n\n", this.xid)
	}
}
