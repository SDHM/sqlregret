package client

import (
	"sqlregret/mysql"
)

type RotateLogEvent struct {
	fileName string
	position int64
}

func ParseRotateLogEvent(logbuf *mysql.LogBuffer, descriptionEvent *FormatDescriptionLogEvent) *RotateLogEvent {

	this := new(RotateLogEvent)
	if descriptionEvent.GetBinlogVer() > 1 {
		this.position = int64(logbuf.GetUInt64())
	}
	this.fileName = logbuf.GetRestString()

	return this
}

func (this *RotateLogEvent) GetFileName() string {
	return this.fileName
}

func (this *RotateLogEvent) GetPosition() int64 {
	return this.position
}
