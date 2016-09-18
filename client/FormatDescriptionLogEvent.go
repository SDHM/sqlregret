package client

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	. "github.com/SDHM/sqlregret/binlogevent"
	"github.com/SDHM/sqlregret/mysql"
)

const (
	SERVER_VERSION = "5.0"
)

type FormatDescriptionLogEvent struct {
	binlogVersion      int     // 2字节
	serverVersion      string  // 50字节
	numberOfEventTypes int     //
	createTimesnamp    uint32  // 创建时间
	PostHeaderLen      []int16 // eventType header length
	checksumAlg        int
	commonHeaderLen    int // eventHeaderLen
	versionSum         int //版本值
}

func ParseFormatDescriptionLogEvent(logBuf *mysql.LogBuffer, descriptionEvent *FormatDescriptionLogEvent) *FormatDescriptionLogEvent {
	this := new(FormatDescriptionLogEvent)
	this.binlogVersion = logBuf.GetUInt16()
	this.serverVersion = logBuf.GetServerVersion()
	this.parseServerVersion()
	this.createTimesnamp = logBuf.GetUInt32()
	this.commonHeaderLen = logBuf.GetUInt8()
	if this.commonHeaderLen < OLD_HEADER_LEN {
		panic(errors.New("Format Description event header length is too short"))
	}

	restlen := logBuf.GetRestLen()
	this.PostHeaderLen = make([]int16, restlen)
	for i := 0; i < restlen; i++ {
		this.PostHeaderLen[i] = int16(logBuf.GetUInt8())
	}

	fmt.Println(this.versionSum, CHECKSUM_VERSION_PRODUCT)
	if this.versionSum < CHECKSUM_VERSION_PRODUCT {
		this.checksumAlg = BINLOG_CHECKSUM_ALG_UNDEF
	} else {
		logBuf.Position(logBuf.GetLength() - BINLOG_CHECKSUM_LEN - 1)
		this.checksumAlg = logBuf.GetInt8()
		fmt.Println("checksumAlg:", this.checksumAlg)
	}
	return this
}

func (this *FormatDescriptionLogEvent) GetChecksumAlg() int {
	return this.checksumAlg
}

func NewFormatDesctiptionLogEvent(binlogVersion int) *FormatDescriptionLogEvent {
	this := new(FormatDescriptionLogEvent)
	this.PostHeaderLen = make([]int16, ENUM_END_EVENT)
	this.binlogVersion = binlogVersion
	switch binlogVersion {
	case 4: /*MySQL 5.0 upper*/
		{
			this.serverVersion = SERVER_VERSION
			this.commonHeaderLen = LOG_EVENT_HEADER_LEN
			this.numberOfEventTypes = LOG_EVENT_TYPES

			//每一种event的herder length
			this.PostHeaderLen = []int16{
				START_V3_HEADER_LEN,
				QUERY_HEADER_LEN,
				STOP_HEADER_LEN,
				ROTATE_HEADER_LEN,
				INTVAR_HEADER_LEN,
				LOAD_HEADER_LEN,
				0, // Unused because the code for Slave log event was removed (15th Oct. 2010)
				CREATE_FILE_HEADER_LEN,
				APPEND_BLOCK_HEADER_LEN,
				EXEC_LOAD_HEADER_LEN,
				DELETE_FILE_HEADER_LEN,
				NEW_LOAD_HEADER_LEN,
				RAND_HEADER_LEN,
				USER_VAR_HEADER_LEN,
				FORMAT_DESCRIPTION_HEADER_LEN,
				XID_HEADER_LEN,
				BEGIN_LOAD_QUERY_HEADER_LEN,
				EXECUTE_LOAD_QUERY_HEADER_LEN,
				TABLE_MAP_HEADER_LEN,
				/*
				   The PRE_GA events are never be written to any binlog, but
				   their lengths are included in Format_description_log_event.
				   Hence, we need to be assign some value here, to avoid reading
				   uninitialized memory when the array is written to disk.
				*/
				0,                  /* PRE_GA_WRITE_ROWS_EVENT */
				0,                  /* PRE_GA_UPDATE_ROWS_EVENT*/
				0,                  /* PRE_GA_DELETE_ROWS_EVENT*/
				ROWS_HEADER_LEN_V1, /* WRITE_ROWS_EVENT_V1*/
				ROWS_HEADER_LEN_V1, /* UPDATE_ROWS_EVENT_V1*/
				ROWS_HEADER_LEN_V1, /* DELETE_ROWS_EVENT_V1*/
				INCIDENT_HEADER_LEN,
				0, /* HEARTBEAT_LOG_EVENT*/
				IGNORABLE_HEADER_LEN,
				IGNORABLE_HEADER_LEN,
				ROWS_HEADER_LEN_V2,
				ROWS_HEADER_LEN_V2,
				ROWS_HEADER_LEN_V2,
				POST_HEADER_LENGTH, /*GTID_EVENT*/
				POST_HEADER_LENGTH, /*ANONYMOUS_GTID_EVENT*/
				IGNORABLE_HEADER_LEN,
				TRANSACTION_CONTEXT_HEADER_LEN,
				VIEW_CHANGE_HEADER_LEN,
				XA_PREPARE_HEADER_LEN,
			}
		}
	default:
		{
			fmt.Println("cur version not support")
		}
	}
	return this
}

func (this *FormatDescriptionLogEvent) GetBinlogVer() int {
	return this.binlogVersion
}

func isDigital(r rune) bool {
	if r >= rune('0') && r <= rune('9') {
		return false
	}
	return true
}

func (this *FormatDescriptionLogEvent) parseServerVersion() {
	splitVersion := strings.Split(this.serverVersion, ".")
	index := strings.IndexFunc(splitVersion[2], isDigital)

	major, _ := strconv.Atoi(splitVersion[0])
	minor, _ := strconv.Atoi(splitVersion[1])
	build, _ := strconv.Atoi(splitVersion[2][0:index])

	this.versionSum = (major*256+minor)*256 + build
}

func (this *FormatDescriptionLogEvent) GetVersionSum() int {
	return this.versionSum
}
