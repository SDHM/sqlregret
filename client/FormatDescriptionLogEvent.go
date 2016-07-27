package client

import (
	"errors"
	"fmt"

	. "github.com/SDHM/sqlregret/mysql"
)

const (
	ENUM_END_EVENT                = 164
	ST_SERVER_VER_LEN             = 50
	LOG_EVENT_HEADER_LEN          = 19
	START_V3_HEADER_LEN           = (2 + ST_SERVER_VER_LEN + 4)
	QUERY_HEADER_MINIMAL_LEN      = (4 + 4 + 1 + 2)
	QUERY_HEADER_LEN              = (QUERY_HEADER_MINIMAL_LEN + 2)
	STOP_HEADER_LEN               = 0
	ROTATE_HEADER_LEN             = 8
	INTVAR_HEADER_LEN             = 0
	LOAD_HEADER_LEN               = (4 + 4 + 4 + 1 + 1 + 4)
	SLAVE_HEADER_LEN              = 0
	CREATE_FILE_HEADER_LEN        = 4
	APPEND_BLOCK_HEADER_LEN       = 4
	EXEC_LOAD_HEADER_LEN          = 4
	DELETE_FILE_HEADER_LEN        = 4
	NEW_LOAD_HEADER_LEN           = LOAD_HEADER_LEN
	RAND_HEADER_LEN               = 0
	USER_VAR_HEADER_LEN           = 0
	FORMAT_DESCRIPTION_HEADER_LEN = (START_V3_HEADER_LEN + 1 + LOG_EVENT_TYPES)
	LOG_EVENT_TYPES               = (ENUM_END_EVENT - 1)
	XID_HEADER_LEN                = 0

	BEGIN_LOAD_QUERY_HEADER_LEN         = APPEND_BLOCK_HEADER_LEN
	EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN = (4 + 4 + 4 + 1)
	EXECUTE_LOAD_QUERY_HEADER_LEN       = (QUERY_HEADER_LEN + EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN)
	TABLE_MAP_HEADER_LEN                = 8
	ROWS_HEADER_LEN_V1                  = 8
	IGNORABLE_HEADER_LEN                = 0
	ROWS_HEADER_LEN_V2                  = 10
	POST_HEADER_LENGTH                  = 11
	ANNOTATE_ROWS_HEADER_LEN            = 0
	BINLOG_CHECKPOINT_HEADER_LEN        = 4
	GTID_HEADER_LEN                     = 19
	OLD_HEADER_LEN                      = 13
	GTID_LIST_HEADER_LEN                = 4
)

const (
	SERVER_VERSION = "5.0"
)

type FormatDescriptionLogEvent struct {
	BinlogVersion      int
	serverVersion      string
	numberOfEventTypes int
	PostHeaderLen      []int16
	commonHeaderLen    int
}

func ParseFormatDescriptionLogEvent(logBuf *LogBuffer, descriptionEvent *FormatDescriptionLogEvent) *FormatDescriptionLogEvent {
	this := new(FormatDescriptionLogEvent)
	this.BinlogVersion = logBuf.GetUInt16()
	this.serverVersion = logBuf.GetServerVersion()
	//timestamp := logbuf.GetUInt32()
	logBuf.SkipLen(4)
	this.commonHeaderLen = logBuf.GetUInt8()
	if this.commonHeaderLen < OLD_HEADER_LEN {
		panic(errors.New("Format Description event header length is too short"))
	}

	restlen := logBuf.GetRestLen()
	this.PostHeaderLen = make([]int16, restlen)
	for i := 0; i < restlen; i++ {
		this.PostHeaderLen[i] = int16(logBuf.GetUInt8())
	}

	return this
}

func NewFormatDesctiptionLogEvent(binlogVersion int) *FormatDescriptionLogEvent {
	this := new(FormatDescriptionLogEvent)
	this.PostHeaderLen = make([]int16, ENUM_END_EVENT)
	this.BinlogVersion = binlogVersion
	switch binlogVersion {
	case 4: /*MySQL 5.0 upper*/
		{
			this.serverVersion = SERVER_VERSION
			this.commonHeaderLen = LOG_EVENT_HEADER_LEN
			this.numberOfEventTypes = LOG_EVENT_TYPES
			/* Note: all event types must explicitly fill in their lengths here. */
			this.PostHeaderLen[START_EVENT_V3-1] = START_V3_HEADER_LEN
			this.PostHeaderLen[QUERY_EVENT-1] = QUERY_HEADER_LEN
			this.PostHeaderLen[STOP_EVENT-1] = STOP_HEADER_LEN
			this.PostHeaderLen[ROTATE_EVENT-1] = ROTATE_HEADER_LEN
			this.PostHeaderLen[INTVAR_EVENT-1] = INTVAR_HEADER_LEN
			this.PostHeaderLen[LOAD_EVENT-1] = LOAD_HEADER_LEN
			this.PostHeaderLen[SLAVE_EVENT-1] = SLAVE_HEADER_LEN
			this.PostHeaderLen[CREATE_FILE_EVENT-1] = CREATE_FILE_HEADER_LEN
			this.PostHeaderLen[APPEND_BLOCK_EVENT-1] = APPEND_BLOCK_HEADER_LEN
			this.PostHeaderLen[EXEC_LOAD_EVENT-1] = EXEC_LOAD_HEADER_LEN
			this.PostHeaderLen[DELETE_FILE_EVENT-1] = DELETE_FILE_HEADER_LEN
			this.PostHeaderLen[NEW_LOAD_EVENT-1] = NEW_LOAD_HEADER_LEN
			this.PostHeaderLen[RAND_EVENT-1] = RAND_HEADER_LEN
			this.PostHeaderLen[USER_VAR_EVENT-1] = USER_VAR_HEADER_LEN
			this.PostHeaderLen[FORMAT_DESCRIPTION_EVENT-1] = FORMAT_DESCRIPTION_HEADER_LEN
			this.PostHeaderLen[XID_EVENT-1] = XID_HEADER_LEN
			this.PostHeaderLen[BEGIN_LOAD_QUERY_EVENT-1] = BEGIN_LOAD_QUERY_HEADER_LEN
			this.PostHeaderLen[EXECUTE_LOAD_QUERY_EVENT-1] = EXECUTE_LOAD_QUERY_HEADER_LEN
			this.PostHeaderLen[TABLE_MAP_EVENT-1] = TABLE_MAP_HEADER_LEN
			this.PostHeaderLen[WRITE_ROWS_EVENTv1-1] = ROWS_HEADER_LEN_V1
			this.PostHeaderLen[UPDATE_ROWS_EVENTv1-1] = ROWS_HEADER_LEN_V1
			this.PostHeaderLen[DELETE_ROWS_EVENTv1-1] = ROWS_HEADER_LEN_V1
			/*
			 * We here have the possibility to simulate a master of before we changed the table map id to be stored
			 * in 6 bytes: when it was stored in 4 bytes (=> post_header_len was 6). This is used to test backward
			 * compatibility. This code can be removed after a few months (today is Dec 21st 2005), when we know
			 * that the 4-byte masters are not deployed anymore (check with Tomas Ulin first!), and the accompanying
			 * test (rpl_row_4_bytes) too.
			 */
			this.PostHeaderLen[HEARTBEAT_EVENT-1] = 0
			this.PostHeaderLen[IGNORABLE_EVENT-1] = IGNORABLE_HEADER_LEN
			this.PostHeaderLen[ROWS_QUERY_EVENT-1] = IGNORABLE_HEADER_LEN
			this.PostHeaderLen[WRITE_ROWS_EVENTv2-1] = ROWS_HEADER_LEN_V2
			this.PostHeaderLen[UPDATE_ROWS_EVENTv2-1] = ROWS_HEADER_LEN_V2
			this.PostHeaderLen[DELETE_ROWS_EVENTv2-1] = ROWS_HEADER_LEN_V2
			this.PostHeaderLen[GTID_EVENT-1] = POST_HEADER_LENGTH
			this.PostHeaderLen[ANONYMOUS_GTID_EVENT-1] = POST_HEADER_LENGTH
			this.PostHeaderLen[PREVIOUS_GTIDS_EVENT-1] = IGNORABLE_HEADER_LEN
			// mariadb 10
			this.PostHeaderLen[ANNOTATE_ROWS_EVENT-1] = ANNOTATE_ROWS_HEADER_LEN
			this.PostHeaderLen[BINLOG_CHECKPOINT_EVENT-1] = BINLOG_CHECKPOINT_HEADER_LEN
			this.PostHeaderLen[GTID_EVENT-1] = GTID_HEADER_LEN
			this.PostHeaderLen[GTID_LIST_EVENT-1] = GTID_LIST_HEADER_LEN
		}
	default:
		{
			fmt.Println("cur version not support")
		}
	}
	return this
}
