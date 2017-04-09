package client

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	. "github.com/SDHM/sqlregret/binlogevent"
	"github.com/SDHM/sqlregret/config"
	"github.com/SDHM/sqlregret/mysql"
	"github.com/SDHM/sqlregret/protocol"
	"github.com/cihub/seelog"
	"github.com/golang/protobuf/proto"
)

type LogParser struct {
	binlogFileName string
	fileIndex      int
	context        *LogContext
	tableMetaCache *TableMetaCache
}

func (this *LogParser) Parse(header *LogHeader, logBuf *mysql.LogBuffer, SwitchFile func(string, int64) error) {

	switch event_type := header.GetEventType(); event_type {
	case ROTATE_EVENT:
		{
			rotateEvent := this.ReadRotateEvent(logBuf)
			SwitchFile(rotateEvent.GetFileName(), rotateEvent.GetPosition())
		}
	case QUERY_EVENT:
		{
			this.ReadQueryEvent(header, logBuf)
		}
	case XID_EVENT:
		{
			this.ReadXidEvent(header, logBuf)
		}
	case TABLE_MAP_EVENT:
		{
			this.ReadTableMapEvent(logBuf)
		}
	case WRITE_ROWS_EVENT_V1, WRITE_ROWS_EVENT:
		{
			// fmt.Println("eventType: WRITE logBuf:", logBuf.GetRestLen())
			this.ReadRowEvent(header, event_type, logBuf)
		}
	case UPDATE_ROWS_EVENT_V1, UPDATE_ROWS_EVENT:
		{
			// fmt.Println("eventType: UPDATE logBuf:", logBuf.GetRestLen())
			this.ReadRowEvent(header, event_type, logBuf)
		}
	case DELETE_ROWS_EVENT_V1, DELETE_ROWS_EVENT:
		{
			// fmt.Println("eventType: DELETE logBuf:", logBuf.GetRestLen())
			this.ReadRowEvent(header, event_type, logBuf)
		}
	case ROWS_QUERY_LOG_EVENT:
		{
			if config.G_filterConfig.Origin {
				this.ReadRowsQueryEvent(header, event_type, logBuf)
			}
		}
	case USER_VAR_EVENT:
		{
			fmt.Println("USER_VAR_EVENT NOT HANDLE")
		}
	case INTVAR_EVENT:
		{
			// fmt.Println("INTVAR_EVENT NOT HANDLE")
		}
	case RAND_EVENT:
		{
			// fmt.Println("RAND_EVENT NOT HANDLE")
		}
	case STOP_EVENT:
		{
			fmt.Println("STOP_EVENT HAPPEND!")
		}
	case FORMAT_DESCRIPTION_EVENT:
		{
			// fmt.Println("FORMAT_DESCRIPTION_EVENT HAPPEND!")
			this.ReadFormatDescriptionEvent(logBuf)
		}
	case GTID_EVENT:
		{
			// fmt.Println("GTID_EVENT NOT HANDLE")
		}
	case GTID_LIST_EVENT:
		{
			// fmt.Println("GTID_LIST_EVENT NOT HANDLE")
		}
	case ANONYMOUS_GTID_LOG_EVENT:
		{
			// fmt.Println("ANONYMOUS_GTID_EVENT NOT HANDLE")
		}
	case PREVIOUS_GTIDS_LOG_EVENT:
		{
			// fmt.Println("PREVIOUS_GTIDS_EVENT NOT HANDLE")
		}
	case GTID_LOG_EVENT:
		{
			// fmt.Println("GTID_LOG_EVENT NOT HANDLE")
		}
	default:
		fmt.Println("接收到未识别的命令头：", event_type)
	}
}

func getEventType(event_type int) protocol.EventType {
	switch event_type {
	case WRITE_ROWS_EVENT_V1, WRITE_ROWS_EVENT:
		return protocol.EventType_INSERT
	case UPDATE_ROWS_EVENT_V1, UPDATE_ROWS_EVENT:
		return protocol.EventType_UPDATE
	case DELETE_ROWS_EVENT_V1, DELETE_ROWS_EVENT:
		return protocol.EventType_DELETE
	default:
		panic(errors.New(fmt.Sprintf("unsupport event type:%x", event_type)))
	}
}

func (this *LogParser) ReadEventHeader(logBuf *mysql.LogBuffer) *LogHeader {
	header := ParseLogHeader(logBuf, this.context.GetFormatDescription())
	return header
}

func (this *LogParser) ReadFormatDescriptionEvent(logbuf *mysql.LogBuffer) {
	descriptionEvent := ParseFormatDescriptionLogEvent(logbuf, this.context.GetFormatDescription())
	this.context.SetFormatDescription(descriptionEvent)
}

func (this *LogParser) ReadQueryEvent(logHeader *LogHeader, logbuf *mysql.LogBuffer) {
	queryEvent := ParseQueryLogEvent(logbuf, this.context.GetFormatDescription())
	switch sql := strings.ToLower(queryEvent.GetQuery()); sql {
	case "begin":
		{
			//fmt.Println("\n开始事务")
			G_transaction.Begin(queryEvent.GetTime(), this.binlogFileName, logHeader.GetLogPos())
		}
	case "commit":
		{
			fmt.Println("提交事务2")
		}
	default:
		{
			//如果开放DDL解析，则解析DDL,否则不解析
			if config.G_filterConfig.WithDDL {
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
					fmt.Printf("修改表结构语句:%s\n", sql)
				} else {
					fmt.Printf("DDL语句:%s\n", sql)
				}
			}

		}
	}

}

func (this *LogParser) ReadTableMapEvent(logbuf *mysql.LogBuffer) {
	tableMapEvent := ParseTableMapLogEvent(logbuf, this.context.GetFormatDescription())
	this.context.PutTable(tableMapEvent)
}

func (this *LogParser) ReadRowsQueryEvent(logHeader *LogHeader, event_type int, logbuf *mysql.LogBuffer) {
	rowsQueryEvent := ParseRowsQueryEvent(logbuf, this.context.GetFormatDescription())
	timeSnap := time.Unix(logHeader.timeSnamp, 0)

	fmt.Printf("时间戳:%s\t原始语句为:%s;\n", timeSnap.Format("2006-01-02 15:04:05"), rowsQueryEvent.GetRowsQueryString())

}

func (this *LogParser) ReadRowEvent(logHeader *LogHeader, event_type int, logbuf *mysql.LogBuffer) {

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

	if event_type == UPDATE_ROWS_EVENT_V1 || event_type == UPDATE_ROWS_EVENT {
		//columns-present-bitmap2 := logbuf.GetVarLenBytes((int(column_count) + 7) / 8)
		logbuf.SkipLen((int(column_count) + 7) / 8)
	}
	tableMapEvent := this.context.GetTable(table_id)
	columns := tableMapEvent.ColumnInfo
	eventType := getEventType(event_type)
	dbName := tableMapEvent.DbName
	tableName := tableMapEvent.TblName

	//数据库过滤
	if config.G_filterConfig.FilterDb != "" {
		if !strings.EqualFold(dbName, config.G_filterConfig.FilterDb) {
			// fmt.Printf("库不同过滤 dbName:%s configDb:%s \n", dbName,
			// 	config.G_filterConfig.FilterDb)
			return
		}
	}

	//表过滤
	if config.G_filterConfig.FilterTable != "" {
		if !strings.EqualFold(tableName, config.G_filterConfig.FilterTable) ||
			!strings.EqualFold(dbName, config.G_filterConfig.FilterDb) {
			// fmt.Printf("表库不同过滤 dbName:%s tableName:%s configDb:%s configTb:%s\n", dbName,
			// 	tableName, config.G_filterConfig.FilterDb, config.G_filterConfig.FilterTable)
			return
		}
	}

	// fmt.Println("dbName:", dbName)
	// fmt.Println("tableName:", tableName)
	// fmt.Println("configDb:", config.G_filterConfig.FilterTable)
	// fmt.Println("configTb:", config.G_filterConfig.FilterDb)

	//列过滤
	tableMeta := this.getTableMeta(tableMapEvent.DbName, tableMapEvent.TblName, false)
	if FilterColumns(eventType, tableMeta, columns) {
		return
	}

	rows := this.ReadRows(logHeader, tableMapEvent, eventType, columns, logbuf)

	row_change := new(protocol.RowChange)
	row_change.SetTableId(table_id)
	row_change.SetEventType(eventType)
	row_change.SetIsDdl(false)
	row_change.SetRowDatas(rows)

	if value, err := proto.Marshal(row_change); nil != err {
		fmt.Println("Marshal failed!", err.Error())
	} else {
		// header := CreateHeader(this.binlogFileName, logHeader, &tableMapEvent.DbName, &tableMapEvent.TblName, &eventType)
		// entry := CreateEntry(header, protocol.EntryType_ROWDATA, value)
		fmt.Sprintf("%s", value[0:1])
	}
}

func (this *LogParser) ReadXidEvent(logHeader *LogHeader, logbuf *mysql.LogBuffer) {
	xid := int64(logbuf.GetUInt64())
	G_transaction.End(xid)
	G_transaction.PrintTransaction()
	// fmt.Printf("提交事务:%d\n\n", xid)
}

func (this *LogParser) ReadRotateEvent(logbuf *mysql.LogBuffer) *RotateLogEvent {
	rotateEvent := ParseRotateLogEvent(logbuf, this.context.GetFormatDescription())
	position := NewBinlogPosition(rotateEvent.GetFileName(), rotateEvent.GetPosition())
	this.context.SetLogPosition(position)
	seelog.Debugf("切换文件:%s\t偏移:%d", rotateEvent.GetFileName(), rotateEvent.GetPosition())
	return rotateEvent
}

func (this *LogParser) ReadRows(
	logHeader *LogHeader,
	tableMapEvent *TableMapLogEvent,
	eventType protocol.EventType,
	columns []*Column,
	logbuf *mysql.LogBuffer) []*protocol.RowData {
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

func (this *LogParser) isSqlTypeString(sqlType JavaType) bool {

	isString := false
	switch sqlType {
	case INTEGER, TINYINT, SMALLINT, BIGINT, BIT:
	case REAL, TIMESTAMP, TIME, DATE, CHAR, VARCHAR, BINARY, VARBINARY, LONGVARBINARY:
		isString = true
	case DOUBLE, DECIMAL:
		isString = false
	default:
		isString = true
	}

	return isString
}

func (this *LogParser) transformToSqlInsert(logHeader *LogHeader, tableMapEvent *TableMapLogEvent, columns []*protocol.Column) {

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
	rstSql := fmt.Sprintf("时间戳:%s\t插入语句为:", timeSnap.Format("2006-01-02 15:04:05"))

	G_transaction.AppendSQL(&timeSnap, NewShowSql(true, rstSql, !config.G_filterConfig.Dump))
	G_transaction.AppendSQL(&timeSnap, NewShowSql(false, sql+";", !config.G_filterConfig.Dump))

	if !config.G_filterConfig.NeedReverse {
		G_transaction.AppendSQL(&timeSnap, NewShowSql(true, "\n", true))
		return
	}

	sql = fmt.Sprintf("delete from %s.%s where ", tableMapEvent.DbName, tableMapEvent.TblName)
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

	rstSql = fmt.Sprintf("\t对应的反向insert语句:")
	G_transaction.AppendSQL(&timeSnap, NewShowSql(true, rstSql, !config.G_filterConfig.Dump))
	G_transaction.AppendSQL(&timeSnap, NewShowSql(false, sql+"\n", true))
}

func (this *LogParser) transformToSqlDelete(logHeader *LogHeader, tableMapEvent *TableMapLogEvent, columns []*protocol.Column) {
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
	rstSql := fmt.Sprintf("时间戳:%s\t删除语句为:", timeSnap.Format("2006-01-02 15:04:05"))

	G_transaction.AppendSQL(&timeSnap, NewShowSql(true, rstSql, !config.G_filterConfig.Dump))
	G_transaction.AppendSQL(&timeSnap, NewShowSql(false, sql, !config.G_filterConfig.Dump))

	if !config.G_filterConfig.NeedReverse {
		G_transaction.AppendSQL(&timeSnap, NewShowSql(true, "\n", true))
		return
	}

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

	rstSql = fmt.Sprintf("\t对应的反向insert语句:")
	G_transaction.AppendSQL(&timeSnap, NewShowSql(true, rstSql, !config.G_filterConfig.Dump))
	G_transaction.AppendSQL(&timeSnap, NewShowSql(false, regretsql+";\n", true))
}

func (this *LogParser) transformToSqlUpdate(logHeader *LogHeader, tableMapEvent *TableMapLogEvent, before []*protocol.Column, after []*protocol.Column) {
	// 更新字段索引
	updateCount := 0
	for index, column := range after {
		if column.GetUpdated() {
			updateCount += 1
		} else {
			if before[index].GetValue() == "" && after[index].GetValue() != "" {
				// fmt.Printf("update1 before:%s \tafter:%s\n", before[index].GetValue(), after[index].GetValue())
				updateCount += 1
			} else if before[index].GetIsNull() && !after[index].GetIsNull() {
				// fmt.Printf("update2 before:%s \tafter:%s\n", before[index].GetValue(), after[index].GetValue())
				updateCount += 1
			} else if !before[index].GetIsNull() && after[index].GetIsNull() {
				updateCount += 1
			} else if before[index].GetValue() != "" && after[index].GetValue() == "" {
				updateCount += 1
			}
		}

		// if (!column.GetUpdated() && (column.GetIsNull() || column.GetValue() == "")) &&
		// 	(!after[index].GetIsNull() || after[index].GetValue() != "") {
		// 	// updateCount += 1
		// }
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

		// if (!column.GetUpdated() && (column.GetIsNull() || column.GetValue() == "")) &&
		// 	(!after[index].GetIsNull() || after[index].GetValue() != "") {

		// if !column.GetUpdated() && ((column.GetIsNull() && !after[index].GetIsNull()) ||
		// 	(column.GetValue() == "" && after[index].GetValue() != "")) {
		// 	updateCount2 += 1

		// 	if updateCount != updateCount2 {

		// 		if this.isSqlTypeString(JavaType(column.GetSqlType())) {
		// 			sql += column.GetName() + "='" + column.GetValue() + "', "
		// 		} else {
		// 			sql += column.GetName() + "=" + column.GetValue() + ", "
		// 		}
		// 	} else {
		// 		if this.isSqlTypeString(JavaType(column.GetSqlType())) {
		// 			sql += column.GetName() + "='" + column.GetValue() + "'"
		// 		} else {
		// 			sql += column.GetName() + "=" + column.GetValue()
		// 		}
		// 	}
		// } else
		if !column.GetUpdated() && (!before[index].GetIsNull() && before[index].GetValue() == "" && after[index].GetValue() != "") {
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
		} else if !column.GetUpdated() && (before[index].GetIsNull() && !after[index].GetIsNull()) {
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
		} else if !before[index].GetIsNull() && after[index].GetIsNull() {
			updateCount2 += 1
			if updateCount != updateCount2 {
				if this.isSqlTypeString(JavaType(column.GetSqlType())) {
					sql += column.GetName() + "=NULL, "
				} else {
					sql += column.GetName() + "=NULL, "
				}
			} else {
				if this.isSqlTypeString(JavaType(column.GetSqlType())) {
					sql += column.GetName() + "=NULL"
				} else {
					sql += column.GetName() + "=NULL"
				}
			}
		} else if before[index].GetValue() != "" && after[index].GetValue() == "" {
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
	rstSql := fmt.Sprintf("时间戳:%s  update语句:", timeSnap.Format("2006-01-02 15:04:05"))

	G_transaction.AppendSQL(&timeSnap, NewShowSql(true, rstSql, !config.G_filterConfig.Dump))
	G_transaction.AppendSQL(&timeSnap, NewShowSql(false, sql+";", !config.G_filterConfig.Dump))

	if !config.G_filterConfig.NeedReverse {
		G_transaction.AppendSQL(&timeSnap, NewShowSql(true, "\n", true))
		return
	}

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

		// if (!column.GetUpdated() && (column.GetIsNull() || column.GetValue() == "")) &&
		// 	(!after[index].GetIsNull() && after[index].GetValue() != "") {
		// 	updateCount2 += 1
		// 	if updateCount != updateCount2 {
		// 		sqlregret += column.GetName() + "=\"\"" + ", "
		// 	} else {
		// 		sqlregret += column.GetName() + "=\"\""
		// 	}
		// }

		if !column.GetUpdated() && (!before[index].GetIsNull() && before[index].GetValue() == "" && after[index].GetValue() != "") {
			updateCount2 += 1
			if updateCount != updateCount2 {
				sqlregret += column.GetName() + "=\"\"" + ", "
			} else {
				sqlregret += column.GetName() + "=\"\""
			}
		} else if !column.GetUpdated() && (before[index].GetIsNull() && !after[index].GetIsNull()) {
			updateCount2 += 1
			if updateCount != updateCount2 {
				sqlregret += column.GetName() + "=NULL" + ", "
			} else {
				sqlregret += column.GetName() + "=NULL"
			}
		} else if !before[index].GetIsNull() && after[index].GetIsNull() {
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
		} else if before[index].GetValue() != "" && after[index].GetValue() == "" {
			updateCount2 += 1
			if this.isSqlTypeString(JavaType(column.GetSqlType())) {
				sqlregret += column.GetName() + "='" + before[index].GetValue() + "'"
			} else {
				sqlregret += column.GetName() + "=" + before[index].GetValue()
			}
		}
	}

	sqlregret += fmt.Sprintf(" where %s=%s", keyName, keyValue)
	rstSql = fmt.Sprintf("\t\t对应的反向update语句:")
	G_transaction.AppendSQL(&timeSnap, NewShowSql(true, rstSql, !config.G_filterConfig.Dump))
	G_transaction.AppendSQL(&timeSnap, NewShowSql(false, sqlregret+";\n", true))
}

func (this *LogParser) fetchValue(logbuf *mysql.LogBuffer, columnType byte, meta int, isBinary bool) (interface{}, JavaType, int) {
	var javaType JavaType
	var length int
	var value interface{}

	if columnType == mysql.MYSQL_TYPE_STRING {
		if meta >= 256 {
			byte0 := meta >> 8
			byte1 := meta & 0xff
			if (byte0 & 0x30) != 0x30 {
				length = byte1 | (((byte0 & 0x30) ^ 0x30) << 4)
				columnType = byte(byte0 | 0x30)
			} else {
				switch byte(byte0) {
				case mysql.MYSQL_TYPE_SET, mysql.MYSQL_TYPE_ENUM, mysql.MYSQL_TYPE_STRING:
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
	case mysql.MYSQL_TYPE_LONG:
		{
			javaType = INTEGER
			value, typeLen = int64(logbuf.GetInt32()), 4
		}
	case mysql.MYSQL_TYPE_TINY:
		{
			javaType = TINYINT
			value, typeLen = int64(logbuf.GetInt8()), 1
		}
	case mysql.MYSQL_TYPE_SHORT:
		{
			javaType = SMALLINT
			value, typeLen = int64(logbuf.GetInt16()), 2
		}
	case mysql.MYSQL_TYPE_INT24:
		{
			javaType = INTEGER
			value, typeLen = int64(logbuf.GetInt24()), 3
		}
	case mysql.MYSQL_TYPE_LONGLONG:
		{
			javaType = BIGINT
			value, typeLen = logbuf.GetInt64(), 8
		}
	case mysql.MYSQL_TYPE_DECIMAL:
		{
			seelog.Debug("MYSQL_TYPE_DECIMAL : This enumeration value is only used internally and cannot exist in a binlog!")
			fmt.Println("decimal")
			javaType = DECIMAL
			value, typeLen = nil, 0
		}
	case mysql.MYSQL_TYPE_NEWDECIMAL:
		{
			precision := meta >> 8
			decimals := meta & 0xff

			value, typeLen = logbuf.GetDecimal(precision, decimals)

			javaType = DECIMAL
			typeLen = precision
			// fmt.Printf("decimal:%v per:%d dec:%d\n", value, precision, decimals)
		}
	case mysql.MYSQL_TYPE_FLOAT:
		{
			javaType = REAL
			value, typeLen = logbuf.GetFloat32(), 4
			fmt.Println("float32:", value)
		}
	case mysql.MYSQL_TYPE_DOUBLE:
		{
			javaType = DOUBLE
			value, typeLen = logbuf.GetFloat64(), 8
		}
	case mysql.MYSQL_TYPE_BIT:
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
	case mysql.MYSQL_TYPE_TIMESTAMP:
		{
			i32 := logbuf.GetUInt32()
			if i32 == 0 {
				value = "0000-00-00 00:00:00"
			} else {
				value = logbuf.GetTimeStringFromUnixTimeStamp(int64(i32))
			}
			javaType, typeLen = TIMESTAMP, 4
		}
	case mysql.MYSQL_TYPE_TIMESTAMP2:
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
	case mysql.MYSQL_TYPE_DATETIME:
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
	case mysql.MYSQL_TYPE_DATETIME2:
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
	case mysql.MYSQL_TYPE_TIME:
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
	case mysql.MYSQL_TYPE_TIME2:
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
	case mysql.MYSQL_TYPE_NEWDATE:
		{
			seelog.Debug("MYSQL_TYPE_NEWDATE : This enumeration value is only used internally and cannot exist in a binlog!")
			javaType = DATE
			value = nil
			typeLen = 0
		}
	case mysql.MYSQL_TYPE_DATE:
		{
			i32 := logbuf.GetUInt24()
			if i32 == 0 {
				value = "0000-00-00"
			} else {
				value = fmt.Sprintf("%04d-%02d-%02d", i32/(16*32), i32/32%16, i32%32)
			}
			javaType, typeLen = DATE, 3
		}
	case mysql.MYSQL_TYPE_YEAR:
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
	case mysql.MYSQL_TYPE_ENUM:
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
	case mysql.MYSQL_TYPE_SET:
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
	case mysql.MYSQL_TYPE_TINY_BLOB:
		{
			seelog.Debug("MYSQL_TYPE_TINY_BLOB : This enumeration value is only used internally and cannot exist in a binlog!")
		}
	case mysql.MYSQL_TYPE_MEDIUM_BLOB:
		{
			seelog.Debug("MYSQL_TYPE_MEDIUM_BLOB : This enumeration value is only used internally and cannot exist in a binlog!")
		}
	case mysql.MYSQL_TYPE_LONG_BLOB:
		{
			seelog.Debug("MYSQL_TYPE_LONG_BLOB : This enumeration value is only used internally and cannot exist in a binlog!")
		}
	case mysql.MYSQL_TYPE_BLOB:
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
	case mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING:
		{
			length = meta
			tmpLen := int(0)
			if length < 256 {
				length = logbuf.GetUInt8()
				tmpLen = 1
			} else {
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
	case mysql.MYSQL_TYPE_STRING:
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
	case mysql.MYSQL_TYPE_GEOMETRY:
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
			seelog.Debug("!! Unsupport column type MYSQL_TYPE_GEOMETRY: meta=%d, len = %d", meta, length)
			javaType = BINARY
			value = bina
			typeLen = length + meta
		}
	default:
		seelog.Debug("!! Don't know how to handle column type=%d meta=%d", javaType, meta)
		javaType = OTHER
		value = nil
		typeLen = 0
	}

	return value, javaType, typeLen
}

func (this *LogParser) ReadRow(
	tableMapEvent *TableMapLogEvent,
	isAfter bool,
	row *protocol.RowData,
	columns []*Column,
	column_mark []byte,
	logbuf *mysql.LogBuffer) []*protocol.Column {
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
			column.SetSqlType(int32(this.mysqlToJavaType(c.ColumnType, c.ColumnMeta, isBinary)))
			column.SetValue("")
			column.SetIsNull(true)
			// fmt.Printf("空列索引:%d\t空列名称:%s\n", i, fieldMeta.ColumnName)
			continue
		} else {
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
					seelog.Debug("unhandle type")
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

func (this *LogParser) isUpdate(beforeColumn []*protocol.Column, newVal *string, index int) bool {
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

func (this *LogParser) getTableMeta(dbName string, tableName string, flush bool) *TableMeta {
	return this.tableMetaCache.getTableMeta(dbName+"."+tableName, flush)
}

func (this *LogParser) mysqlToJavaType(columnType byte, meta int, isBinary bool) JavaType {
	var javaType JavaType

	if columnType == mysql.MYSQL_TYPE_STRING {
		if meta >= 256 {
			byte0 := meta >> 8
			if (byte0 & 0x30) != 0x30 {
				/* a long CHAR() field: see #37426 */
				columnType = byte(byte0 | 0x30)
			} else {
				switch byte(byte0) {
				case mysql.MYSQL_TYPE_SET, mysql.MYSQL_TYPE_ENUM, mysql.MYSQL_TYPE_STRING:
					columnType = byte(byte0)
				}
			}
		}
	}

	switch columnType {
	case mysql.MYSQL_TYPE_LONG:
		javaType = INTEGER
	case mysql.MYSQL_TYPE_TINY:
		javaType = TINYINT
	case mysql.MYSQL_TYPE_SHORT:
		javaType = SMALLINT
	case mysql.MYSQL_TYPE_INT24:
		javaType = INTEGER
	case mysql.MYSQL_TYPE_LONGLONG:
		javaType = BIGINT
	case mysql.MYSQL_TYPE_DECIMAL:
		javaType = DECIMAL
	case mysql.MYSQL_TYPE_NEWDECIMAL:
		javaType = DECIMAL
	case mysql.MYSQL_TYPE_FLOAT:
		javaType = REAL
	case mysql.MYSQL_TYPE_DOUBLE:
		javaType = DOUBLE
	case mysql.MYSQL_TYPE_BIT:
		javaType = BIT

	case mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_DATETIME:
		javaType = TIMESTAMP
	case mysql.MYSQL_TYPE_TIME:
		javaType = TIME

	case mysql.MYSQL_TYPE_NEWDATE, mysql.MYSQL_TYPE_DATE:
		javaType = DATE
	case mysql.MYSQL_TYPE_YEAR:
		javaType = VARCHAR
	case mysql.MYSQL_TYPE_ENUM:
		javaType = INTEGER
	case mysql.MYSQL_TYPE_SET:
		javaType = BINARY
	case mysql.MYSQL_TYPE_TINY_BLOB, mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB:
		if meta == 1 {
			javaType = VARBINARY
		} else {
			javaType = LONGVARBINARY
		}
	case mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING:
		if isBinary {
			// varbinary在binlog中为var_string类型
			javaType = VARBINARY
		} else {
			javaType = VARCHAR
		}
	case mysql.MYSQL_TYPE_STRING:
		if isBinary {
			// binary在binlog中为string类型
			javaType = BINARY
		} else {
			javaType = CHAR
		}
	case mysql.MYSQL_TYPE_GEOMETRY:
		javaType = BINARY
	default:
		javaType = OTHER
	}

	return javaType
}
