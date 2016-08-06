package client

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	. "github.com/SDHM/sqlregret/mysql"
	"github.com/SDHM/sqlregret/protocol"
	"github.com/golang/protobuf/proto"
)

const (
	DATETIMEF_INT_OFS int64 = 0x8000000000
	TIMEF_INT_OFS     int64 = 0x800000
)

type EntryFunc func(transaction *protocol.Entry)

func CreateHeader(binlogFile string,
	logHeader *LogHeader,
	schemaName *string,
	tableName *string,
	eventType *protocol.EventType) *protocol.Header {
	this := new(protocol.Header)
	this.SetVersion(1)
	this.SetLogFileName(binlogFile)
	this.SetLogfileOffset(logHeader.GetLogPos() - logHeader.GetEventLen())
	this.SetServerId(logHeader.GetServerId())
	this.SetServerCode("UTF-8")
	this.SetExecuteTime(logHeader.GetExecuteTime() * 1000)
	this.SetSourceType(protocol.Type_MYSQL)
	this.SetEventLength(logHeader.GetEventLen())

	if nil != eventType {
		this.SetEventType(*eventType)
	}

	if nil != schemaName {
		this.SetSchemaName(*schemaName)
	}

	if nil != tableName {
		this.SetTableName(*tableName)
	}

	return this
}

func CreateTransactionBegin(threadId int64) *protocol.TransactionBegin {
	this := new(protocol.TransactionBegin)
	this.SetThreadId(threadId)
	return this
}

func CreateTransactionEnd(transactionId int64) *protocol.TransactionEnd {
	this := new(protocol.TransactionEnd)
	this.SetTransactionId(strconv.FormatInt(transactionId, 10))
	return this
}

func CreateEntry(header *protocol.Header,
	entryType protocol.EntryType,
	storeValue []byte) *protocol.Entry {
	this := new(protocol.Entry)
	this.SetHeader(header)
	this.SetEntryType(entryType)
	this.SetStoreValue(storeValue)
	return this
}

func getEventType(event_type int) protocol.EventType {
	switch event_type {
	case WRITE_ROWS_EVENTv0, WRITE_ROWS_EVENTv1, WRITE_ROWS_EVENTv2:
		return protocol.EventType_INSERT
	case UPDATE_ROWS_EVENTv0, UPDATE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv2:
		return protocol.EventType_UPDATE
	case DELETE_ROWS_EVENTv0, DELETE_ROWS_EVENTv1, DELETE_ROWS_EVENTv2:
		return protocol.EventType_DELETE
	default:
		panic(errors.New(fmt.Sprintf("unsupport event type:%x", event_type)))
	}
}

func (this *MysqlConnection) Register() error {
	this.pkg.Sequence = 0

	data := make([]byte, 4, 18+len(this.self_name)+len(this.self_user)+len(this.self_password))

	var master_server_id uint32 = 1

	data = append(data, COM_REGISTER_SLAVE)
	data = append(data, Uint32ToBytes(this.self_slaveId)...)
	data = append(data, byte(len(this.self_name)))
	data = append(data, []byte(this.self_name)...)
	data = append(data, byte(len(this.self_user)))
	data = append(data, []byte(this.self_user)...)
	data = append(data, byte(len(this.self_password)))
	data = append(data, []byte(this.self_password)...)
	data = append(data, Uint16ToBytes(this.self_port)...)
	data = append(data, []byte{0, 0, 0, 0}...)
	data = append(data, Uint32ToBytes(master_server_id)...)

	if err := this.writePacket(data); err != nil {
		return err
	}

	if _, err := this.readPacket(); err != nil {
		this.logger.Error(err.Error())
		return err
	} else {
		//fmt.Println(by)
	}

	return nil
}

func (this *MysqlConnection) Dump(position uint32, filename string) error {
	this.pkg.Sequence = 0

	data := make([]byte, 4, 11+len(filename))

	data = append(data, COM_BINLOG_DUMP)
	data = append(data, Uint32ToBytes(position)...)
	data = append(data, Uint16ToBytes(uint16(0))...)
	data = append(data, Uint32ToBytes(this.self_slaveId)...)
	data = append(data, []byte(filename)...)

	if err := this.writePacket(data); err != nil {
		this.logger.Error(err.Error())
		return err
	}

	this.binlogFileName = filename
	this.context = NewLogContext()

	for {
		if by, err := this.readPacket(); err != nil {
			this.logger.Error(err.Error())
			return err
		} else {
			fmt.Println("len:", len(by))
			header := this.ReadEventHeader(NewLogBuffer(by[1:20]))

			switch event_type := header.GetEventType(); event_type {
			case ROTATE_EVENT:
				{
					this.ReadRotateEvent(NewLogBuffer(by[20:]))
				}
			case QUERY_EVENT:
				{
					this.ReadQueryEvent(header, NewLogBuffer(by[20:]))
				}
			case XID_EVENT:
				{
					this.ReadXidEvent(header, NewLogBuffer(by[20:]))
				}
			case TABLE_MAP_EVENT:
				{
					this.ReadTableMapEvent(NewLogBuffer(by[20:]))
				}
			case WRITE_ROWS_EVENTv1, WRITE_ROWS_EVENTv2:
				{
					this.ReadRowEvent(header, event_type, NewLogBuffer(by[20:]))
				}
			case UPDATE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv2:
				{
					this.ReadRowEvent(header, event_type, NewLogBuffer(by[20:]))
				}
			case DELETE_ROWS_EVENTv1, DELETE_ROWS_EVENTv2:
				{
					this.ReadRowEvent(header, event_type, NewLogBuffer(by[20:]))
				}
			case ROWS_QUERY_EVENT:
				{
					fmt.Println("ROWS_QUERY_EVENT NOT HANDLE")
				}
			case USER_VAR_EVENT:
				{
					fmt.Println("USER_VAR_EVENT NOT HANDLE")
				}
			case INTVAR_EVENT:
				{
					fmt.Println("INTVAR_EVENT NOT HANDLE")
				}
			case RAND_EVENT:
				{
					fmt.Println("RAND_EVENT NOT HANDLE")
				}
			case STOP_EVENT:
				{
					this.logger.Debug("stop\n")
				}
			case FORMAT_DESCRIPTION_EVENT:
				{
					this.ReadFormatDescriptionEvent(NewLogBuffer(by[20:]))
				}
			default:
				fmt.Println("接收到未识别的命令头：", event_type)
			}
		}
	}

	return nil
}

func (this *MysqlConnection) ReadEventHeader(logBuf *LogBuffer) *LogHeader {
	header := ParseLogHeader(logBuf, this.context.GetFormatDescription())
	return header
}

func (this *MysqlConnection) ReadFormatDescriptionEvent(logbuf *LogBuffer) {
	descriptionEvent := ParseFormatDescriptionLogEvent(logbuf, this.context.GetFormatDescription())
	this.context.SetFormatDescription(descriptionEvent)
}

func (this *MysqlConnection) ReadQueryEvent(logHeader *LogHeader, logbuf *LogBuffer) {
	queryEvent := ParseQueryLogEvent(logbuf, this.context.GetFormatDescription())
	switch sql := strings.ToLower(queryEvent.GetQuery()); sql {
	case "begin":
		{
			transactionBegin := CreateTransactionBegin(queryEvent.GetSessionId())

			if value, err := proto.Marshal(transactionBegin); nil != err {
				fmt.Println("Marshal failed!", err.Error())
			} else {
				header := CreateHeader(this.binlogFileName, logHeader, nil, nil, nil)
				entry := CreateEntry(header, protocol.EntryType_TRANSACTIONBEGIN, value)
				fmt.Sprintf("%s", entry.GetHeader().GetLogfileName())
			}

			// fmt.Println("开始事务:", sql)
		}
	case "commit":
		{
			// fmt.Println("提交事务:", sql)
		}
	default:
		{
			if strings.Contains(sql, "alter table") {
				s := strings.Split(sql, " ")
				for index := range s {
					if s[index] == "table" {
						if index+1 < len(s) {
							tableName := s[index+1]
							this.getTableMeta(queryEvent.GetSchema(), tableName, true)
						}
						break
					}
				}
				// fmt.Printf("alter语句:\n%s\n", sql)
			} else {
				// fmt.Printf("DDL语句:\n%s\n", sql)
			}
		}
	}

}

func (this *MysqlConnection) ReadTableMapEvent(logbuf *LogBuffer) {
	tableMapEvent := ParseTableMapLogEvent(logbuf, this.context.GetFormatDescription())
	this.context.PutTable(tableMapEvent)
}

func (this *MysqlConnection) ReadRowEvent(logHeader *LogHeader, event_type int, logbuf *LogBuffer) {

	descriptionEvent := this.context.GetFormatDescription()
	postHeaderLen := descriptionEvent.PostHeaderLen[event_type-1]

	var table_id int64
	if postHeaderLen == 6 {
		table_id = int64(logbuf.GetUInt32())
	} else {
		table_id = int64(logbuf.GetUInt48())
	}

	//flags
	logbuf.SkipLen(2)

	if postHeaderLen == ROWS_HEADER_LEN_V2 {
		extra_data_len := logbuf.GetUInt16()
		logbuf.SkipLen(extra_data_len - 2)
	}

	column_count, _ := logbuf.GetVarLen()

	//columns-present-bitmap1 := logbuf.GetVarLenBytes((int(column_count) + 7) / 8)
	logbuf.SkipLen((int(column_count) + 7) / 8)

	if event_type == UPDATE_ROWS_EVENTv1 || event_type == UPDATE_ROWS_EVENTv2 {
		//columns-present-bitmap2 := logbuf.GetVarLenBytes((int(column_count) + 7) / 8)
		logbuf.SkipLen((int(column_count) + 7) / 8)
	}
	tableMapEvent := this.context.GetTable(table_id)
	columns := tableMapEvent.ColumnInfo
	eventType := getEventType(event_type)

	rows := this.ReadRows(logHeader, tableMapEvent, eventType, columns, logbuf)

	row_change := new(protocol.RowChange)
	row_change.SetTableId(table_id)
	row_change.SetEventType(eventType)
	row_change.SetIsDdl(false)
	row_change.SetRowDatas(rows)

	if value, err := proto.Marshal(row_change); nil != err {
		fmt.Println("Marshal failed!", err.Error())
	} else {
		header := CreateHeader(this.binlogFileName, logHeader, &tableMapEvent.DbName, &tableMapEvent.TblName, &eventType)
		entry := CreateEntry(header, protocol.EntryType_ROWDATA, value)
		fmt.Sprintf("%s", entry.GetHeader().GetLogfileName())
	}
}

func (this *MysqlConnection) ReadXidEvent(logHeader *LogHeader, logbuf *LogBuffer) {
	xid := int64(logbuf.GetUInt64())
	end := CreateTransactionEnd(xid)

	if value, err := proto.Marshal(end); nil != err {

	} else {
		header := CreateHeader(this.binlogFileName, logHeader, nil, nil, nil)
		entry := CreateEntry(header, protocol.EntryType_TRANSACTIONEND, value)
		fmt.Sprintf("%s", entry.GetHeader().GetLogfileName())
	}

}

func (this *MysqlConnection) ReadRotateEvent(logbuf *LogBuffer) {
	rotateEvent := ParseRotateLogEvent(logbuf, this.context.GetFormatDescription())
	position := NewBinlogPosition(rotateEvent.GetFileName(), rotateEvent.GetPosition())
	this.context.SetLogPosition(position)
}

func (this *MysqlConnection) ReadRows(logHeader *LogHeader, tableMapEvent *TableMapLogEvent, eventType protocol.EventType, columns []*Column, logbuf *LogBuffer) []*protocol.RowData {
	count := len(columns)
	rows := make([]*protocol.RowData, 0)
	var restlen int = logbuf.GetRestLength()
	for ; restlen > 0; restlen = logbuf.GetRestLength() {
		row_bitmap1 := logbuf.GetVarLenBytes((int(count) + 7) / 8)
		row := new(protocol.RowData)

		if eventType == protocol.EventType_INSERT {
			row.AfterColumns = this.ReadRow(tableMapEvent, true, row, columns, row_bitmap1, logbuf)
			this.transformToSqlInsert(logHeader, tableMapEvent, row.AfterColumns)
		} else if eventType == protocol.EventType_DELETE {
			row.BeforeColumns = this.ReadRow(tableMapEvent, false, row, columns, row_bitmap1, logbuf)
			this.transformToSqlDelete(logHeader, tableMapEvent, row.BeforeColumns)
		} else if eventType == protocol.EventType_UPDATE {
			row.BeforeColumns = this.ReadRow(tableMapEvent, false, row, columns, row_bitmap1, logbuf)
			row_bitmap2 := logbuf.GetVarLenBytes((int(count) + 7) / 8)
			row.AfterColumns = this.ReadRow(tableMapEvent, true, row, columns, row_bitmap2, logbuf)
			this.transformToSqlUpdate(logHeader, tableMapEvent, row.BeforeColumns, row.AfterColumns)
		}

		rows = append(rows, row)
	}
	return rows
}

func (this *MysqlConnection) isSqlTypeString(sqlType JavaType) bool {

	isString := false
	switch sqlType {
	case INTEGER, TINYINT, SMALLINT, BIGINT, BIT:
	case REAL, DOUBLE, DECIMAL, TIMESTAMP, TIME, DATE, CHAR, VARCHAR, BINARY, VARBINARY, LONGVARBINARY:
		isString = true
	default:
		isString = true
	}

	return isString
}

func (this *MysqlConnection) transformToSqlInsert(logHeader *LogHeader, tableMapEvent *TableMapLogEvent, columns []*protocol.Column) {

	sql := fmt.Sprintf("insert into %s.%s(", tableMapEvent.DbName, tableMapEvent.TblName)
	column_len := len(columns)
	for index, column := range columns {
		if index == column_len-1 {
			sql += column.GetName() + ") values("
		} else {
			sql += column.GetName() + ","
		}
	}

	for index, column := range columns {
		if index == column_len-1 {
			if this.isSqlTypeString(JavaType(column.GetSqlType())) {
				sql += "'" + column.GetValue() + "')"
			} else {
				sql += column.GetValue() + ")"
			}
		} else {
			if this.isSqlTypeString(JavaType(column.GetSqlType())) {
				sql += "'" + column.GetValue() + "',"
			} else {
				sql += column.GetValue() + ","
			}
		}
	}

	timeSnap := time.Unix(logHeader.timeSnamp, 0)
	fmt.Printf("时间戳:%s\t插入语句为:%s\n\n", timeSnap.Format("2006-01-02 15:04:05"), sql)
}

func (this *MysqlConnection) transformToSqlDelete(logHeader *LogHeader, tableMapEvent *TableMapLogEvent, columns []*protocol.Column) {
	sql := fmt.Sprintf("delete from %s.%s where ", tableMapEvent.DbName, tableMapEvent.TblName)

	for _, column := range columns {
		if column.GetIsKey() {
			sql += column.GetName() + "="

			if this.isSqlTypeString(JavaType(column.GetSqlType())) {
				sql += `'` + column.GetValue() + "';"
			} else {
				sql += column.GetValue() + ";"
			}

			break
		}
	}

	timeSnap := time.Unix(logHeader.timeSnamp, 0)
	fmt.Printf("时间戳:%s\t删除语句为:%s\n", timeSnap.Format("2006-01-02 15:04:05"), sql)

	regretsql := fmt.Sprintf("insert into %s.%s(", tableMapEvent.DbName, tableMapEvent.TblName)
	column_len := len(columns)
	for index, column := range columns {
		if index == column_len-1 {
			regretsql += column.GetName() + ") values("
		} else {
			regretsql += column.GetName() + ","
		}
	}

	for index, column := range columns {
		if index == column_len-1 {

			if this.isSqlTypeString(JavaType(column.GetSqlType())) {
				regretsql += `'` + column.GetValue() + "')"
			} else {
				regretsql += column.GetValue() + ")"
			}
		} else {
			if this.isSqlTypeString(JavaType(column.GetSqlType())) {
				regretsql += "'" + column.GetValue() + "',"
			} else {
				regretsql += column.GetValue() + ","
			}
		}
	}

	fmt.Printf("时间戳:%s\t对应的反向插入语句为:%s\n\n", timeSnap.Format("2006-01-02 15:04:05"), regretsql)

}

func (this *MysqlConnection) transformToSqlUpdate(logHeader *LogHeader, tableMapEvent *TableMapLogEvent, before []*protocol.Column, after []*protocol.Column) {
	// 更新字段索引
	updateCount := 0
	for index, column := range after {
		if column.GetUpdated() {
			updateCount += 1
		}

		if (!column.GetUpdated() && (column.GetIsNull() || column.GetValue() == "")) &&
			(!after[index].GetIsNull() || after[index].GetValue() != "") {
			updateCount += 1
		}
	}

	keyName := ""
	keyValue := ""
	updateCount2 := 0
	sql := fmt.Sprintf("update %s.%s set ", tableMapEvent.DbName, tableMapEvent.TblName)
	for index, column := range after {

		if column.GetUpdated() {
			updateCount2 += 1
			if updateCount != updateCount2 {
				if this.isSqlTypeString(JavaType(column.GetSqlType())) {
					sql += column.GetName() + "='" + column.GetValue() + "', "
				} else {
					sql += column.GetName() + "=" + column.GetValue() + ", "
				}
			} else {
				if this.isSqlTypeString(JavaType(column.GetSqlType())) {
					sql += column.GetName() + "='" + column.GetValue() + "' "
				} else {
					sql += column.GetName() + "=" + column.GetValue()
				}

			}
		}

		if (!column.GetUpdated() && (column.GetIsNull() || column.GetValue() == "")) &&
			(!after[index].GetIsNull() || after[index].GetValue() != "") {
			updateCount2 += 1

			if updateCount != updateCount2 {

				if this.isSqlTypeString(JavaType(column.GetSqlType())) {
					sql += column.GetName() + "='" + column.GetValue() + "', "
				} else {
					sql += column.GetName() + "=" + column.GetValue() + ", "
				}
			} else {
				if this.isSqlTypeString(JavaType(column.GetSqlType())) {
					sql += column.GetName() + "='" + column.GetValue() + "'"
				} else {
					sql += column.GetName() + "=" + column.GetValue()
				}
			}
		}

		if column.GetIsKey() {
			keyValue = column.GetValue()
			keyName = column.GetName()
		}
	}

	sql += fmt.Sprintf(" where %s=%s", keyName, keyValue)

	timeSnap := time.Unix(logHeader.timeSnamp, 0)
	fmt.Printf("时间戳:%s\tupdate 语句:%s\n", timeSnap.Format("2006-01-02 15:04:05"), sql)

	updateCount2 = 0
	sqlregret := fmt.Sprintf("update %s.%s set ", tableMapEvent.DbName, tableMapEvent.TblName)
	for index, column := range after {
		if column.GetUpdated() {
			updateCount2 += 1
			if updateCount != updateCount2 {

				if this.isSqlTypeString(JavaType(column.GetSqlType())) {
					sqlregret += column.GetName() + "='" + before[index].GetValue() + "', "
				} else {
					sqlregret += column.GetName() + "=" + before[index].GetValue() + ", "
				}
			} else {
				if this.isSqlTypeString(JavaType(column.GetSqlType())) {
					sqlregret += column.GetName() + "='" + before[index].GetValue() + "'"
				} else {
					sqlregret += column.GetName() + "=" + before[index].GetValue()
				}
			}
		}

		if (!column.GetUpdated() && (column.GetIsNull() || column.GetValue() == "")) &&
			(!after[index].GetIsNull() && after[index].GetValue() != "") {
			updateCount2 += 1
			if updateCount != updateCount2 {
				sqlregret += column.GetName() + "=\"\"" + ", "
			} else {
				sqlregret += column.GetName() + "=\"\""
			}
		}
	}

	sqlregret += fmt.Sprintf(" where %s=%s", keyName, keyValue)

	fmt.Printf("时间戳:%s\t对应的反向 update 语句:%s\n\n", timeSnap.Format("2006-01-02 15:04:05"), sqlregret)
}

func (this *MysqlConnection) fetchValue(logbuf *LogBuffer, columnType byte, meta int, isBinary bool) (interface{}, JavaType, int) {
	var javaType JavaType
	var length int
	var value interface{}

	if columnType == MYSQL_TYPE_STRING {
		if meta >= 256 {
			byte0 := meta >> 8
			byte1 := meta & 0xff
			if (byte0 & 0x30) != 0x30 {
				length = byte1 | (((byte0 & 0x30) ^ 0x30) << 4)
				columnType = byte(byte0 | 0x30)
			} else {
				switch byte(byte0) {
				case MYSQL_TYPE_SET, MYSQL_TYPE_ENUM, MYSQL_TYPE_STRING:
					columnType = byte(byte0)
					length = byte1
				default:
					panic(errors.New(fmt.Sprintf(`"!! Don't know how to handle column 
					type=%d meta=%d"`, columnType, meta)))
				}
			}
		} else {
			length = meta
		}
	}

	var typeLen int
	switch columnType {
	case MYSQL_TYPE_LONG:
		{
			javaType = INTEGER
			value, typeLen = int64(logbuf.GetInt32()), 4
		}
	case MYSQL_TYPE_TINY:
		{
			javaType = TINYINT
			value, typeLen = int64(logbuf.GetInt8()), 1
		}
	case MYSQL_TYPE_SHORT:
		{
			javaType = SMALLINT
			value, typeLen = int64(logbuf.GetInt16()), 2
		}
	case MYSQL_TYPE_INT24:
		{
			javaType = INTEGER
			value, typeLen = int64(logbuf.GetInt24()), 3
		}
	case MYSQL_TYPE_LONGLONG:
		{
			javaType = BIGINT
			value, typeLen = logbuf.GetInt64(), 8
		}
	case MYSQL_TYPE_DECIMAL:
		{
			this.logger.Debug("MYSQL_TYPE_DECIMAL : This enumeration value is only used internally and cannot exist in a binlog!")
			javaType = DECIMAL
			value, typeLen = nil, 0
		}
	case MYSQL_TYPE_NEWDECIMAL:
		{
			precision := meta >> 8
			decimals := meta & 0xff
			javaType = DECIMAL
			value, typeLen = logbuf.GetDecimal(precision, decimals)
		}
	case MYSQL_TYPE_FLOAT:
		{
			javaType = REAL
			value, typeLen = logbuf.GetFloat32(), 4
		}
	case MYSQL_TYPE_DOUBLE:
		{
			javaType = DOUBLE
			value, typeLen = logbuf.GetFloat64(), 8
		}
	case MYSQL_TYPE_BIT:
		{
			/* Meta-data: bit_len, bytes_in_rec, 2 bytes */
			nbits := ((meta >> 8) * 8) + (meta & 0xff)
			length = (nbits + 7) / 8
			if nbits > 1 {
				switch length {
				case 1:
					value = int64(logbuf.GetInt8())
				case 2:
					value = int64(logbuf.GetBeUInt16())
				case 3:
					value = int64(logbuf.GetBeUInt24())
				case 4:
					value = int64(logbuf.GetBeUInt32())
				case 5:
					value = int64(logbuf.GetBeUInt40())
				case 6:
					value = int64(logbuf.GetBeUInt48())
				case 7:
					value = int64(logbuf.GetBeUInt56())
				case 8:
					value = int64(logbuf.GetBeUInt64())
				default:
					panic(errors.New("!! Unknown Bit len"))
				}
			} else {
				value = int64(logbuf.GetInt8())
			}
			javaType = BIT
			typeLen = nbits
		}
	case MYSQL_TYPE_TIMESTAMP:
		{
			i32 := logbuf.GetUInt32()
			if i32 == 0 {
				value = "0000-00-00 00:00:00"
			} else {
				value = logbuf.GetTimeStringFromUnixTimeStamp(int64(i32))
			}
			javaType, typeLen = TIMESTAMP, 4
		}
	case MYSQL_TYPE_TIMESTAMP2:
		{
			tv_sec := logbuf.GetBeUInt32()
			tv_usec := 0
			switch meta {
			case 0:
				tv_usec = 0
			case 1, 2:
				tv_usec = logbuf.GetInt8()
				tv_usec = tv_usec * 10000
			case 3, 4:
				tv_usec = logbuf.GetBeInt16()
				tv_usec = tv_usec * 100
			case 5, 6:
				tv_usec = logbuf.GetBeInt24()
			default:
				tv_usec = 0
			}

			if tv_sec == 0 {
				value = "0000-00-00 00:00:00"
			} else {
				value = logbuf.GetTimeStringFromUnixTimeStamp(int64(tv_sec * 1000))
			}
			javaType = TIMESTAMP
			typeLen = 4 + (meta+1)/2
		}
	case MYSQL_TYPE_DATETIME:
		{
			// MYSQL DataTypes: DATETIME
			// range is '0000-01-01 00:00:00' to '9999-12-31 23:59:59'
			i64 := logbuf.GetInt64()
			if i64 == 0 {
				value = "0000-00-00 00:00:00"
			} else {
				d := int(i64 / 1000000)
				t := int(i64 % 1000000)
				value = fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", d/10000, (d%10000)/100, d%100, t/10000, (t%10000)/100, t%100)
			}
			javaType, typeLen = TIMESTAMP, 8
		}
	case MYSQL_TYPE_DATETIME2:
		{
			intpart := logbuf.GetBeUInt40()
			intpart -= DATETIMEF_INT_OFS // big-endian
			frac := 0
			switch meta {
			case 0:
				frac = 0
			case 1, 2:
				frac = logbuf.GetInt8()
				frac *= 10000
			case 3, 4:
				frac = logbuf.GetBeInt16()
				frac *= 100
			case 5, 6:
				frac = logbuf.GetBeInt24()
				frac *= 100
			default:
				frac = 0
			}

			if intpart == 0 {
				value = "0000-00-00 00:00:00"
			} else {
				// 构造TimeStamp只处理到秒
				ymd := intpart >> 17
				ym := ymd >> 5
				hms := intpart % (1 << 17)

				// if (cal == null) cal = Calendar.getInstance();
				// cal.clear();
				// cal.set((int) (ym / 13), (int) (ym % 13) - 1, (int) (ymd
				// % (1 << 5)), (int) (hms >> 12),
				// (int) ((hms >> 6) % (1 << 6)), (int) (hms % (1 << 6)));
				// value = new Timestamp(cal.getTimeInMillis());
				value = fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", int(ym/13), int(ym%13), int(ymd%(1<<5)), int(hms>>12), int((hms>>6)%(1<<6)), int(hms%(1<<6)))
			}

			javaType = TIMESTAMP
			typeLen = 5 + (meta+1)/2
		}
	case MYSQL_TYPE_TIME:
		{
			// MYSQL DataTypes: TIME
			// The range is '-838:59:59' to '838:59:59'
			// final int i32 = buffer.getUint24();
			i32 := logbuf.GetInt24()
			u32 := int32(math.Abs(float64(i32)))
			if i32 == 0 {
				value = "00:00:00"
			} else {
				if i32 >= 0 {
					value = fmt.Sprintf("%s%02d:%02d:%02d", "", int32(u32/10000), int32((u32%10000)/100), int32(u32%100))
				} else {
					value = fmt.Sprintf("%s%02d:%02d:%02d", "-", int32(u32/10000), int32((u32%10000)/100), int32(u32%100))
				}
			}
			javaType, typeLen = TIME, 3
		}
	case MYSQL_TYPE_TIME2:
		{
			intpart := int64(0)
			frac := int(0)
			ltime := int64(0)
			switch meta {
			case 0:
				{
					intpart = int64(logbuf.GetBeInt24()) - TIMEF_INT_OFS
					ltime = intpart << 24
				}
			case 1, 2:
				{

					intpart = int64(logbuf.GetBeInt24()) - TIMEF_INT_OFS
					frac = logbuf.GetUInt8()
					if intpart < 0 && frac > 0 {

						intpart++
						frac -= 0x100
					}
					ltime = int64(intpart<<24) + int64(frac*10000)
				}
			case 3, 4:
				{
					intpart = int64(logbuf.GetBeUInt24()) - TIMEF_INT_OFS
					frac = logbuf.GetBeUInt16()
					if intpart < 0 && frac > 0 {
						/*
						 * Fix reverse fractional part order:
						 * "0x10000 - frac". See comments for FSP=1 and
						 * FSP=2 above.
						 */
						intpart++
						frac -= 0x10000
						// fraclong = frac * 100;
					}
					ltime = int64(intpart<<24) + int64(frac*100)
				}

			case 5, 6:
				{
					intpart = int64(logbuf.GetBeUInt48()) - TIMEF_INT_OFS
					ltime = intpart
				}

			default:
				{
					intpart = int64(logbuf.GetBeUInt24()) - TIMEF_INT_OFS
					ltime = int64(intpart << 24)
				}

			}

			if intpart == 0 {
				fmt.Println("intpart value")
				value = "00:00:00"
			} else {
				// 目前只记录秒，不处理us frac
				ultime := int64(math.Abs(float64(ltime)))

				intpart = int64(ultime >> 24)

				var tmpstr string
				if ltime >= 0 {
					tmpstr = ""
				} else {
					tmpstr = "-"
				}
				value = fmt.Sprintf("%s%02d:%02d:%02d", tmpstr, int((intpart>>12)%(1<<10)), int((intpart>>6)%(1<<6)), int(intpart%(1<<6)))
			}
			javaType = TIME

			typeLen = 3 + (meta+1)/2
		}
	case MYSQL_TYPE_NEWDATE:
		{
			this.logger.Debug("MYSQL_TYPE_NEWDATE : This enumeration value is only used internally and cannot exist in a binlog!")
			javaType = DATE
			value = nil
			typeLen = 0
		}
	case MYSQL_TYPE_DATE:
		{
			i32 := logbuf.GetUInt24()
			if i32 == 0 {
				value = "0000-00-00"
			} else {
				value = fmt.Sprintf("%04d-%02d-%02d", i32/(16*32), i32/32%16, i32%32)
			}
			javaType, typeLen = DATE, 3
		}
	case MYSQL_TYPE_YEAR:
		{
			i32 := logbuf.GetUInt8()
			// The else, value is java.lang.Short.
			if i32 == 0 {
				value = "0000"
			} else {
				value = strconv.Itoa(i32 + 1900)
			}
			javaType, typeLen = VARCHAR, 1
		}
	case MYSQL_TYPE_ENUM:
		{
			i32 := int(0)

			switch length {
			case 1:
				i32 = logbuf.GetUInt8()
			case 2:
				i32 = logbuf.GetUInt16()
			default:
				panic(errors.New(fmt.Sprintf("!! Unknown ENUM packlen=%d", length)))
			}
			value = int64(i32)
			javaType, typeLen = INTEGER, length
		}
	case MYSQL_TYPE_SET:
		{
			nbits := (meta & 0xff) * 8
			length = (nbits + 7) / 8

			if nbits > 1 {
				switch length {
				case 1:
					value = int64(logbuf.GetUInt8())
					break
				case 2:
					value = int64(logbuf.GetUInt16())
				case 3:
					value = int64(logbuf.GetUInt24())
				case 4:
					value = int64(logbuf.GetUInt32())
				case 5:
					value = int64(logbuf.GetUInt40())
				case 6:
					value = int64(logbuf.GetUInt48())
				case 7:
					value = int64(logbuf.GetUInt56())
				case 8:
					value = int64(logbuf.GetUInt64())
				default:
					panic(errors.New(fmt.Sprintf("!! Unknown Set len = %d", length)))
				}
			} else {
				value = int64(logbuf.GetInt8())
			}

			javaType, typeLen = BIT, length
		}
	case MYSQL_TYPE_TINY_BLOB:
		{
			this.logger.Debug("MYSQL_TYPE_TINY_BLOB : This enumeration value is only used internally and cannot exist in a binlog!")
		}
	case MYSQL_TYPE_MEDIUM_BLOB:
		{
			this.logger.Debug("MYSQL_TYPE_MEDIUM_BLOB : This enumeration value is only used internally and cannot exist in a binlog!")
		}
	case MYSQL_TYPE_LONG_BLOB:
		{
			this.logger.Debug("MYSQL_TYPE_LONG_BLOB : This enumeration value is only used internally and cannot exist in a binlog!")
		}
	case MYSQL_TYPE_BLOB:
		{
			/*
			 * BLOB or TEXT datatype
			 */
			switch meta {
			case 1:
				{
					/* TINYBLOB/TINYTEXT */
					len8 := logbuf.GetUInt8()
					bina := make([]byte, 0, len8)
					bina = append(bina, logbuf.GetVarLenBytes(len8)...)
					value = bina
					javaType = VARBINARY
					typeLen = len8 + 1
				}
			case 2:
				{
					/* BLOB/TEXT */
					len16 := logbuf.GetUInt16()
					bina := make([]byte, 0, len16)
					bina = append(bina, logbuf.GetVarLenBytes(len16)...)
					value = bina

					javaType = LONGVARBINARY
					typeLen = len16 + 2
				}
			case 3:
				{
					len24 := logbuf.GetUInt24()

					bina := make([]byte, 0, len24)
					bina = append(bina, logbuf.GetVarLenBytes(len24)...)
					value = bina

					javaType = LONGVARBINARY
					typeLen = len24 + 3
				}
			case 4:
				{
					/* LONGBLOB/LONGTEXT */
					len32 := logbuf.GetUInt32()
					bina := make([]byte, 0, len32)
					bina = append(bina, logbuf.GetVarLenBytes(int(len32))...)
					value = bina

					javaType = LONGVARBINARY
					typeLen = int(len32) + 4
				}
			default:
				panic(errors.New(fmt.Sprintf("!! Unknown BLOB packlen = %d", meta)))
			}
		}
	case MYSQL_TYPE_VARCHAR, MYSQL_TYPE_VAR_STRING:
		{
			length = meta
			tmpLen := int(0)
			if length < 256 {
				//fmt.Println("length < 256 by[0]", by[pos:pos+1])
				length = logbuf.GetUInt8()
				tmpLen = 1
			} else {
				//fmt.Println("by[0]", by[pos:pos+1])
				length = logbuf.GetUInt16()
				tmpLen = 2
			}

			if isBinary {
				bina := make([]byte, 0, length)
				bina = append(bina, logbuf.GetVarLenBytes(length)...)
				javaType = VARBINARY
				value = bina
			} else {
				value = logbuf.GetVarLenString(length)
				javaType = VARCHAR
			}

			typeLen = length + tmpLen

		}
	case MYSQL_TYPE_STRING:
		{
			tmpLen := 0
			if length < 256 {
				length = logbuf.GetUInt8()
				tmpLen = 1
			} else {
				length = logbuf.GetUInt16()
				tmpLen = 2
			}

			if isBinary {
				/* fill binary */

				bina := make([]byte, 0, length)
				bina = append(bina, logbuf.GetVarLenBytes(length)...)
				javaType = BINARY
				value = bina
			} else {
				value = logbuf.GetVarLenString(length)
				javaType = CHAR // Types.VARCHAR;
			}
			typeLen = length + tmpLen
		}
	case MYSQL_TYPE_GEOMETRY:
		{
			/*
			 * MYSQL_TYPE_GEOMETRY: copy from BLOB or TEXT
			 */
			switch meta {
			case 1:
				length = logbuf.GetUInt8()
			case 2:
				length = logbuf.GetUInt16()
			case 3:
				length = logbuf.GetUInt24()
			case 4:
				tmpLength := logbuf.GetUInt32()
				length = int(tmpLength)
			default:
				panic(errors.New(fmt.Sprintf("!! Unknown MYSQL_TYPE_GEOMETRY packlen = %d", meta)))
			}
			/* fill binary */

			bina := make([]byte, 0, length)
			bina = append(bina, logbuf.GetVarLenBytes(length)...)
			/* Warning unsupport cloumn type */
			this.logger.Debug("!! Unsupport column type MYSQL_TYPE_GEOMETRY: meta=%d, len = %d", meta, length)
			javaType = BINARY
			value = bina
			typeLen = length + meta
		}
	default:
		this.logger.Debug("!! Don't know how to handle column type=%d meta=%d", javaType, meta)
		javaType = OTHER
		value = nil
		typeLen = 0
	}

	return value, javaType, typeLen
}

func (this *MysqlConnection) ReadRow(tableMapEvent *TableMapLogEvent, isAfter bool, row *protocol.RowData, columns []*Column, column_mark []byte, logbuf *LogBuffer) []*protocol.Column {
	tableMeta := this.getTableMeta(tableMapEvent.DbName, tableMapEvent.TblName, false)
	if nil == tableMeta {
		err := errors.New("not found [" + tableMapEvent.DbName + "." + tableMapEvent.TblName + "] in db , pls check!")
		fmt.Println(err.Error())
	}

	pro_columns := make([]*protocol.Column, 0, 10)

	for i, c := range columns {
		column := new(protocol.Column)

		var fieldMeta *FieldMeta = nil
		var isBinary bool = false
		if nil != tableMeta {
			fieldMeta = tableMeta.Fileds[i]
			column.SetMysqlType(fieldMeta.ColumnType)
			column.SetName(fieldMeta.ColumnName)
			column.SetIsKey(fieldMeta.IsThisKey())
			isBinary = fieldMeta.IsBinary()
		}
		column.SetIndex(int32(i))

		if IsNull(column_mark, i) {
			column.SetIsNull(true)
			pro_columns = append(pro_columns, column)
			//fmt.Printf("第%d列值为空; ", i)
			column.SetSqlType(int32(this.mysqlToJavaType(c.ColumnType, c.ColumnMeta, isBinary)))
			column.SetValue("")
			column.SetIsNull(true)
			//fmt.Printf("null index:%d\tcolumn name:%s\n", i, fieldMeta.ColumnName)
			continue
		} else {
			//fmt.Printf("index:%d\tcolumn name:%s\n", i, fieldMeta.ColumnName)
			column.SetIsNull(false)
		}

		value, javaType, typeLen := this.fetchValue(logbuf, c.ColumnType, c.ColumnMeta, isBinary)
		column.SetLength(int32(typeLen))

		switch javaType {
		case INTEGER, TINYINT, SMALLINT, BIGINT:
			{
				//del with unsigned
				if nil != fieldMeta && fieldMeta.IsThisUnsigned() && value.(int64) < 0 {
					column.SetValue("")
				} else {
					column.SetValue(strconv.FormatInt(value.(int64), 10))
				}
			}
		case REAL, DOUBLE, DECIMAL, TIMESTAMP, TIME, DATE, CHAR, VARCHAR:
			{
				switch value.(type) {
				case string:
					column.SetValue(value.(string))
				case nil:
					column.SetValue("")
				default:
					column.SetValue("")
					this.logger.Debug("unhandle type")
				}
			}
		case BIT:
			{
				column.SetValue(strconv.FormatInt(value.(int64), 10))
			}
		case BINARY, VARBINARY, LONGVARBINARY:
			{
				column.SetValue(string(value.([]byte)))
				if nil != fieldMeta && fieldMeta.IsThisText() {
					javaType = CLOB
				} else {
					javaType = BLOB
				}
			}
		default:
			{
				switch value.(type) {
				case string:
					{
						column.SetValue(value.(string))
					}
				default:
					column.SetValue("")
				}

			}
		}
		column.SetSqlType(int32(javaType))
		column.SetUpdated(isAfter && this.isUpdate(row.BeforeColumns, column.Value, i))
		// fmt.Println("column: ", i, column)
		pro_columns = append(pro_columns, column)
	}
	return pro_columns
}

func (this *MysqlConnection) isUpdate(beforeColumn []*protocol.Column, newVal *string, index int) bool {
	if len(beforeColumn) == 0 {
		return false //panic(errors.New("ERROR ## the bfColumns is null"))
	}
	if index < 0 {
		return false
	}

	if (len(beforeColumn) - 1) < index {
		return false
	}

	column := beforeColumn[index]
	if nil == column {
		return true
	} else {
		if column.Value != nil && nil != newVal {
			if *column.Value != *newVal {
				return true
			}
		}

	}

	return false
}

func (this *MysqlConnection) getTableMeta(dbName string, tableName string, flush bool) *TableMeta {
	return this.tableMetaCache.getTableMeta(dbName+"."+tableName, flush)
}

func (this *MysqlConnection) mysqlToJavaType(columnType byte, meta int, isBinary bool) JavaType {
	var javaType JavaType

	if columnType == MYSQL_TYPE_STRING {
		if meta >= 256 {
			byte0 := meta >> 8
			if (byte0 & 0x30) != 0x30 {
				/* a long CHAR() field: see #37426 */
				columnType = byte(byte0 | 0x30)
			} else {
				switch byte(byte0) {
				case MYSQL_TYPE_SET, MYSQL_TYPE_ENUM, MYSQL_TYPE_STRING:
					columnType = byte(byte0)
				}
			}
		}
	}

	switch columnType {
	case MYSQL_TYPE_LONG:
		javaType = INTEGER
	case MYSQL_TYPE_TINY:
		javaType = TINYINT
	case MYSQL_TYPE_SHORT:
		javaType = SMALLINT
	case MYSQL_TYPE_INT24:
		javaType = INTEGER
	case MYSQL_TYPE_LONGLONG:
		javaType = BIGINT
	case MYSQL_TYPE_DECIMAL:
		javaType = DECIMAL
	case MYSQL_TYPE_NEWDECIMAL:
		javaType = DECIMAL
	case MYSQL_TYPE_FLOAT:
		javaType = REAL
	case MYSQL_TYPE_DOUBLE:
		javaType = DOUBLE
	case MYSQL_TYPE_BIT:
		javaType = BIT

	case MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_DATETIME:
		javaType = TIMESTAMP
	case MYSQL_TYPE_TIME:
		javaType = TIME

	case MYSQL_TYPE_NEWDATE, MYSQL_TYPE_DATE:
		javaType = DATE
	case MYSQL_TYPE_YEAR:
		javaType = VARCHAR
	case MYSQL_TYPE_ENUM:
		javaType = INTEGER
	case MYSQL_TYPE_SET:
		javaType = BINARY
	case MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_BLOB:
		if meta == 1 {
			javaType = VARBINARY
		} else {
			javaType = LONGVARBINARY
		}
	case MYSQL_TYPE_VARCHAR, MYSQL_TYPE_VAR_STRING:
		if isBinary {
			// varbinary在binlog中为var_string类型
			javaType = VARBINARY
		} else {
			javaType = VARCHAR
		}
	case MYSQL_TYPE_STRING:
		if isBinary {
			// binary在binlog中为string类型
			javaType = BINARY
		} else {
			javaType = CHAR
		}
	case MYSQL_TYPE_GEOMETRY:
		javaType = BINARY
	default:
		javaType = OTHER
	}

	return javaType
}
