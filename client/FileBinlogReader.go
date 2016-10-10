package client

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/SDHM/sqlregret/binlogevent"
	"github.com/SDHM/sqlregret/config"
	. "github.com/SDHM/sqlregret/mysql"
	"github.com/cihub/seelog"
)

var (
	binlogFileHeader []byte = []byte{0xfe, 0x62, 0x69, 0x6e}
	lastLogTime             = time.Unix(0, 0)
)

type FileBinlogReader struct {
	LogParser
	fileName  string // binlog文件名
	pos       int32  //当前位置
	dbName    string
	indexFile string
	basePath  string
	reader    io.Reader
	fileArray []string
	index     int
}

func NewFileBinlogReader(dbName string, indexFile string, basePath string) *FileBinlogReader {
	this := new(FileBinlogReader)
	this.dbName = dbName
	this.indexFile = indexFile
	this.basePath = basePath
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

	indexFileName := fmt.Sprintf("%s/%s", this.basePath, this.indexFile)
	f, err := os.Open(indexFileName)
	if nil != err {
		return err
	}

	reader := bufio.NewReader(f)
	this.fileArray = make([]string, 0)
	index := 0
	for {
		if line, _, err := reader.ReadLine(); nil != err {
			break
		} else {
			logFile := strings.Split(string(line), "/")[1]
			if logFile == filename {
				this.index = index
			}
			this.fileArray = append(this.fileArray, logFile)
			index++
		}
	}

	if err := this.changeBinlogFile(position, filename); nil != err {
		seelog.Error("打开文件失败:", err.Error())
		return err
	}

	for {
		if headBuf, err := this.ReadHeader(); nil == err {
			logBBF := NewLogBuffer(headBuf)
			if nil == logBBF {
				seelog.Error("日志记录为空")
			}
			header := this.ReadEventHeader(logBBF)

			//判断截止时间

			if by, err := this.ReadPacket(header.GetEventLen() - binlogevent.LOG_EVENT_HEADER_LEN); nil != err {
				seelog.Error("read packet faield!", err.Error())
				// this.SwitchLogFile(this.fileArray[this.index+1], 4)
			} else {
				timeSnap := time.Unix(header.timeSnamp, 0)

				if config.G_filterConfig.Mode == "mark" {
					this.StoreTimePos(timeSnap, this.binlogFileName, header.GetLogPos())
				}

				if config.G_filterConfig.StartTimeEnable() && config.G_filterConfig.EndTimeEnable() {
					//开始时间和结束时间都设置了
					if timeSnap.After(config.G_filterConfig.StartTime) && timeSnap.Before(config.G_filterConfig.EndTime) {
						//时间在两者之间才能解析

					} else {
						//时间在两者之外，并且不是修改操作的直接跳过
						eventType := header.GetEventType()
						if eventType == binlogevent.WRITE_ROWS_EVENT_V1 || eventType == binlogevent.WRITE_ROWS_EVENT ||
							eventType == binlogevent.UPDATE_ROWS_EVENT_V1 || eventType == binlogevent.UPDATE_ROWS_EVENT ||
							eventType == binlogevent.DELETE_ROWS_EVENT_V1 || eventType == binlogevent.DELETE_ROWS_EVENT {
							continue
						}
					}
				} else {
					if config.G_filterConfig.Mode == "mark" {
						eventType := header.GetEventType()
						if eventType == binlogevent.FORMAT_DESCRIPTION_EVENT {
						} else {
							continue
						}
					}
				}

				//非截止时间
				if header.GetEventType() == FORMAT_DESCRIPTION_EVENT {
					this.Parse(header, NewLogBuffer(by), this.SwitchLogFile)
				} else {
					if this.context.formatDescription.GetChecksumAlg() == binlogevent.BINLOG_CHECKSUM_ALG_CRC32 {
						// fmt.Println("crc32 eventLen:", header.GetEventLen())
						if header.GetEventLen() > 24 {
							this.Parse(header, NewLogBuffer(by[:header.GetEventLen()-binlogevent.BINLOG_CHECKSUM_LEN-binlogevent.LOG_EVENT_HEADER_LEN]), this.SwitchLogFile)
						} else {
							this.Parse(header, NewLogBuffer(by), this.SwitchLogFile)
						}
					} else {
						// fmt.Printf("notcrc eventLen:%d\t checksumalg:%d\n", header.GetEventLen(), this.context.formatDescription.GetChecksumAlg())
						this.Parse(header, NewLogBuffer(by), this.SwitchLogFile)
					}
				}
			}

		} else if err == io.EOF {
			if this.index+1 <= len(this.fileArray) {
				this.changeBinlogFile(4, this.fileArray[this.index])
			} else {
				seelog.Debug("到达最后一个文件")
				break
			}
		}
	}
	return nil
}

func (this *FileBinlogReader) StoreTimePos(t time.Time, fileName string, pos int64) {
	//十秒钟一个记录
	if t.Sub(lastLogTime) >= time.Second*10 {
		str := fmt.Sprintf("时间:%s\t文件名:%s\t位置:%d", t.Format("2006-01-02 15:04:05"), fileName, pos)
		fmt.Println(str)
		lastLogTime = t
	}
}

func (this *FileBinlogReader) changeBinlogFile(position uint32, filename string) error {
	if this.reader != nil {
		if f := this.reader.(*os.File); nil != f {
			f.Close()
			this.reader = nil
		}
	}

	filename = fmt.Sprintf("%s/%s", this.basePath, filename)
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
	this.index = this.index + 1

	seelog.Debug("切换文件:", filename)
	return nil
}

func (this *FileBinlogReader) ReadHeader() ([]byte, error) {
	headBuf := make([]byte, 19)
	if n, err := io.ReadFull(this.reader, headBuf); err == io.EOF && n == 19 {
		return headBuf, nil
	} else if err == io.EOF {
		// fmt.Println("End Of file!")
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
	} else if nil != err && err == io.EOF {
		seelog.Error("there is something wrong!", err.Error())
		return nil, err
	} else {
		return logBuf, nil
	}
}

func (this *FileBinlogReader) SwitchLogFile(fileName string, pos int64) error {
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
