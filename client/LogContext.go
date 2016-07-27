package client

type LogContext struct {
	mapofTable        map[int64]*TableMapLogEvent
	formatDescription *FormatDescriptionLogEvent
	logPosition       *BinlogPosition
}

func NewLogContext() *LogContext {
	formatDescription := NewFormatDesctiptionLogEvent(4)
	return NewLogContextWithDescription(formatDescription)
}

func NewLogContextWithDescription(formatDescription *FormatDescriptionLogEvent) *LogContext {
	this := new(LogContext)
	this.formatDescription = formatDescription
	this.mapofTable = map[int64]*TableMapLogEvent{}
	return this
}

func (this *LogContext) SetLogPosition(logPosition *BinlogPosition) {
	this.logPosition = logPosition
}

func (this *LogContext) GetLogPosition() *BinlogPosition {
	return this.logPosition
}

func (this *LogContext) GetFormatDescription() *FormatDescriptionLogEvent {
	return this.formatDescription
}

func (this *LogContext) SetFormatDescription(formatDescription *FormatDescriptionLogEvent) {
	this.formatDescription = formatDescription
}

func (this *LogContext) PutTable(mapEvent *TableMapLogEvent) {
	this.mapofTable[mapEvent.TableID] = mapEvent
}

func (this *LogContext) GetTable(tableID int64) *TableMapLogEvent {
	if value, ok := this.mapofTable[tableID]; ok {
		return value
	} else {
		return nil
	}
}
