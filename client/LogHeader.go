package client

import (
	"github.com/SDHM/sqlregret/mysql"
)

type LogHeader struct {
	eventType int
	logPos    int64
	timeSnamp int64
	eventLen  int64
	serverId  int64
}

func ParseLogHeader(logBuf *mysql.LogBuffer, descriptionEvent *FormatDescriptionLogEvent) *LogHeader {
	this := new(LogHeader)
	this.timeSnamp = int64(logBuf.GetUInt32())
	this.eventType = logBuf.GetUInt8()
	this.serverId = int64(logBuf.GetUInt32())
	this.eventLen = int64(logBuf.GetInt32())

	if descriptionEvent.GetBinlogVer() > 1 {
		this.logPos = int64(logBuf.GetUInt32())
		//flags
		logBuf.SkipLen(2)
	}

	return this
}

func (this *LogHeader) GetEventType() int {
	return this.eventType
}

func (this *LogHeader) GetLogPos() int64 {
	return this.logPos
}

func (this *LogHeader) GetEventLen() int64 {
	return this.eventLen
}

func (this *LogHeader) GetServerId() int64 {
	return this.serverId
}

func (this *LogHeader) GetExecuteTime() int64 {
	return this.timeSnamp
}
