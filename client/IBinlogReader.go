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

	// //Dump之前要做的操作
	// PreDump() error

	//Dump日志
	Dump(position uint32, filename string) error

	//读取日志头
	ReadHeader() ([]byte, error)

	//读取数据包
	ReadPacket(eventLen int64) ([]byte, error)

	//切换日志文件
	SwitchLogFile(fileName string, pos int64) error

	//关闭连接
	Close() error

	SetTableMetaCache(tableMetaCache *TableMetaCache)
	//查询
	Query(sql string) (*Result, error)
}
