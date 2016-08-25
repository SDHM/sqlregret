package client

import (
	. "github.com/SDHM/sqlregret/mysql"
)

type IBinlogReader interface {
	//建立连接
	Connect() error

	//重新连接
	ReConnect() error

	//注册为从库
	Register() error

	//Dump日志
	Dump(position uint32, filename string) error

	//关闭连接
	Close() error

	SetTableMetaCache(tableMetaCache *TableMetaCache)
	//查询
	Query(sql string) (*Result, error)
}
