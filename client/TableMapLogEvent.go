package client

import . "github.com/SDHM/sqlregret/mysql"

type Column struct {
	ColumnType byte
	ColumnMeta int
}

func IsNull(column_mark []byte, index int) bool {
	b := column_mark[index/8]
	bit := 1 << uint(index&7)
	return b&byte(bit) != 0
}

type TableMapLogEvent struct {
	DbName     string
	TblName    string
	ColumnCnt  int
	ColumnInfo []*Column
	TableID    int64
	NullBitset []byte
}

func ParseTableMapLogEvent(logbuf *LogBuffer,
	descriptionEvent *FormatDescriptionLogEvent) *TableMapLogEvent {
	this := new(TableMapLogEvent)

	postHeaderLen := descriptionEvent.PostHeaderLen[TABLE_MAP_EVENT-1]

	if postHeaderLen == 6 {
		this.TableID = int64(logbuf.GetUInt32())
	} else {
		this.TableID = int64(logbuf.GetUInt48())
	}

	//flags
	logbuf.SkipLen(2)

	schema_length := logbuf.GetUInt8()

	this.DbName = logbuf.GetVarLenString(schema_length)

	//00
	logbuf.SkipLen(1)

	table_length := logbuf.GetUInt8()

	this.TblName = logbuf.GetVarLenString(table_length)

	//00
	logbuf.SkipLen(1)

	columnCntTemp, _ := logbuf.GetVarLen()
	this.ColumnCnt = int(columnCntTemp)
	this.ColumnInfo = make([]*Column, this.ColumnCnt)
	for i := 0; i < this.ColumnCnt; i++ {
		info := new(Column)
		info.ColumnType = logbuf.GetByte()
		this.ColumnInfo[i] = info
	}

	if logbuf.HasMore() {
		fieldSize, _ := logbuf.GetVarLen()
		this.decodeFields(logbuf, fieldSize)
		this.NullBitset = logbuf.GetVarLenBytes(int((this.ColumnCnt + 7) / 8))
	}

	return this
}

func (this *TableMapLogEvent) decodeFields(logbuf *LogBuffer, fieldSize uint64) {
	for i := 0; i < this.ColumnCnt; i++ {
		info := this.ColumnInfo[i]

		switch info.ColumnType {
		case MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB,
			MYSQL_TYPE_DOUBLE, MYSQL_TYPE_FLOAT, MYSQL_TYPE_GEOMETRY:
			{
				info.ColumnMeta = logbuf.GetUInt8()
			}
		case MYSQL_TYPE_SET, MYSQL_TYPE_ENUM:
			{
				x := logbuf.GetUInt8() << 8
				x += logbuf.GetUInt8()
				info.ColumnMeta = x
				// fmt.Printf("This enumeration value is only used internally and cannot exist in a binlog: type=%d\n", int(info.ColumnType))
			}
		case MYSQL_TYPE_STRING, MYSQL_TYPE_NEWDECIMAL:
			{
				x := logbuf.GetUInt8() << 8
				x += logbuf.GetUInt8()
				info.ColumnMeta = x
			}
		case MYSQL_TYPE_BIT, MYSQL_TYPE_VARCHAR:
			{
				info.ColumnMeta = logbuf.GetUInt16()
			}
		case MYSQL_TYPE_TIME2, MYSQL_TYPE_DATETIME2, MYSQL_TYPE_TIMESTAMP2:
			{
				info.ColumnMeta = logbuf.GetUInt8()
			}
		default:
			info.ColumnMeta = 0
		}
	}
}
