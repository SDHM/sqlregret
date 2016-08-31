package client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	. "github.com/SDHM/sqlregret/mysql"
	"github.com/cihub/seelog"
)

var (
	binlogFileHeader []byte = []byte{0xfe, 0x62, 0x69, 0x6e}
)

type FileBinlogReader struct {
	LogParser
	fileName string // binlog文件名
	pos      int32  //当前位置
	dbName   string
	reader   io.Reader
}

func NewFileBinlogReader(dbName string) *FileBinlogReader {
	this := new(FileBinlogReader)
	this.dbName = dbName
	this.context = NewLogContext()
	return this
}

//建立连接
func (this *FileBinlogReader) Connect() error {

	return nil
}

//重新连接
func (this *FileBinlogReader) ReConnect() error {
	return nil
}

//注册为从库
func (this *FileBinlogReader) Register() error {
	return nil
}

//Dump日志
func (this *FileBinlogReader) Dump(position uint32, filename string) error {

	f, err := os.Open(filename)
	if nil != err {
		return err
	}

	b := make([]byte, 4)
	if _, err = f.Read(b); err != nil {
		return err
	} else if !bytes.Equal(b, binlogFileHeader) {
		return errors.New(filename + " is not a valid binlog file, head 4 bytes must fe'bin' ")
	}

	this.reader = f

	for {
		if headBuf, err := this.ReadHeader(); nil == err {
			logBBF := NewLogBuffer(headBuf)
			if nil == logBBF {
				fmt.Println("logbuf is nil ")
			}
			logHeader := this.ReadEventHeader(logBBF)

			if by, err := this.ReadPacket(logHeader.GetEventLen() - 19); nil != err {
				fmt.Println("error")
			} else {
				this.Parse(logHeader, NewLogBuffer(by))
			}
		} else if err == io.EOF {
			break
		}
	}
	return nil
}

func (this *FileBinlogReader) ReadHeader() ([]byte, error) {
	headBuf := make([]byte, 19)
	if n, err := io.ReadFull(this.reader, headBuf); err == io.EOF && n == 19 {
		seelog.Error("h there is something wrong3!")
		return headBuf, nil
	} else if nil != err && n != 19 {
		seelog.Error("End Of file!", err.Error())
		return nil, err
	} else {
		return headBuf, nil
	}
}

func (this *FileBinlogReader) ReadPacket(eventLen int64) ([]byte, error) {
	if eventLen == 0 {
		seelog.Error("read 0 byte not allowed!")
		return nil, errors.New("read 0 byte not allowed!")
	}

	logBuf := make([]byte, eventLen)
	if n, err := io.ReadFull(this.reader, logBuf); err == io.EOF && n == int(eventLen) {
		seelog.Error("there is something wrong3!")
		return logBuf, nil
	} else if nil != err {
		seelog.Error("there is something wrong!", err.Error())
		return nil, err
	} else {
		return logBuf, nil
	}
}

func (this *FileBinlogReader) SwitchLogFile(fileName string, pos int) error {
	this.binlogFileName = fileName
	return nil
}

//关闭连接
func (this *FileBinlogReader) Close() error {
	return nil
}

func (this *FileBinlogReader) SetTableMetaCache(tableMetaCache *TableMetaCache) {
	this.tableMetaCache = tableMetaCache
}

//查询
func (this *FileBinlogReader) Query(sql string) (*Result, error) {
	return nil, nil
}
