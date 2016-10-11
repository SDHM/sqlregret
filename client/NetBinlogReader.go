package client

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/SDHM/sqlregret/binlogevent"
	. "github.com/SDHM/sqlregret/mysql"
	"github.com/cihub/seelog"
)

const (
	DATETIMEF_INT_OFS int64 = 0x8000000000
	TIMEF_INT_OFS     int64 = 0x800000

	BINLOG_DUMP_NON_BLOCK           uint16 = 1
	BINLOG_SEND_ANNOTATE_ROWS_EVENT uint16 = 2

	pingPeriod = 10
)

func NewNetBinlogReader(
	masterAddr, user, password, dbName string,
	port uint16,
	slaveId uint32) *NetBinlogReader {
	this := new(NetBinlogReader)
	this.addr = masterAddr + ":" + strconv.Itoa(int(port))
	this.self_user = user
	this.user = user
	this.password = password
	this.self_password = password
	this.db = dbName
	this.self_slaveId = slaveId
	this.self_port = port
	this.self_name = "tsg-" + strconv.Itoa(int(slaveId))
	//use utf8
	this.collation = DEFAULT_COLLATION_ID
	this.charset = DEFAULT_CHARSET
	this.context = NewLogContext()
	this.binlogFileName = ""
	return this
}

type NetBinlogReader struct {
	LogParser
	conn          net.Conn
	pkg           *PacketIO
	addr          string
	user          string
	password      string
	db            string
	self_name     string
	self_user     string
	self_password string
	self_port     uint16
	self_slaveId  uint32
	capability    uint32
	status        uint16
	collation     CollationId
	charset       string
	salt          []byte
	lastPing      int64
	pkgErr        error
}

func (this *NetBinlogReader) Connect() error {
	return this.ReConnect()
}

func (this *NetBinlogReader) ReConnect() error {
	if this.conn != nil {
		this.conn.Close()
	}

	n := "tcp"
	if strings.Contains(this.addr, "/") {
		n = "unix"
	}
	seelog.Debug("begin connect server !")
	netConn, err := net.Dial(n, this.addr)
	if err != nil {
		seelog.Error(err.Error())
		return err
	}
	seelog.Debug("Connect server succeed!")
	this.conn = netConn
	this.pkg = NewPacketIO(netConn, netConn)

	if err := this.readInitialHandshake(); err != nil {
		this.conn.Close()
		seelog.Error(err.Error())
		return err
	}

	if err := this.writeAuthHandshake(); err != nil {
		this.conn.Close()
		seelog.Error(err.Error())
		return err
	}

	if _, err := this.readOK(); err != nil {
		this.conn.Close()
		seelog.Error(err.Error())
		return err
	}

	//we must always use autocommit
	if !this.IsAutoCommit() {
		if _, err := this.exec("set autocommit = 1"); err != nil {
			this.conn.Close()
			seelog.Error(err.Error())
			return err
		}
	}

	this.lastPing = time.Now().Unix()

	return nil
}

func (this *NetBinlogReader) Close() error {
	if this.conn != nil {
		this.conn.Close()
		this.conn = nil
	}

	return nil
}

func (this *NetBinlogReader) Register() error {

	this.Execute(`set @master_binlog_checksum= '@@global.binlog_checksum'`)

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
		seelog.Error(err.Error())
		return err
	} else {
		//fmt.Println(by)
	}

	return nil
}

func (this *NetBinlogReader) Dump(position uint32, filename string) error {
	this.pkg.Sequence = 0

	data := make([]byte, 4, 11+len(filename))

	data = append(data, COM_BINLOG_DUMP)
	data = append(data, Uint32ToBytes(position)...)
	data = append(data, Uint16ToBytes(uint16(0))...)
	data = append(data, Uint32ToBytes(this.self_slaveId)...)
	data = append(data, []byte(filename)...)

	if err := this.writePacket(data); err != nil {
		seelog.Error(err.Error())
		return err
	}
	this.binlogFileName = filename
	this.ParseBinlog()

	return nil
}

func (this *NetBinlogReader) ParseBinlog() error {
	for {
		if by, err := this.ReadPacket(0); err != nil {
			seelog.Error(err.Error())
			return err
		} else {
			header := this.ReadEventHeader(NewLogBuffer(by[1:20]))

			timeSnap := time.Unix(header.timeSnamp, 0)
			if FilterTime(timeSnap, header.GetEventType()) {
				continue
			}

			if FilterMode(header.GetEventType()) {
				this.StoreTimePos(timeSnap, this.binlogFileName, header.GetLogPos())
				continue
			}

			if FilterPos(header.GetEventType(), this.fileIndex, header.GetLogPos()) {
				fmt.Println("过滤了3")
				continue
			}

			if FilterSkipSQL(header.GetEventType()) {
				continue
			}

			this.ParseLog(header, by[0:])
		}
	}
}

func (this *NetBinlogReader) ParseLog(header *LogHeader, by []byte) {
	if header.GetEventType() == FORMAT_DESCRIPTION_EVENT {
		this.Parse(header, NewLogBuffer(by[20:]), this.SwitchLogFile)
	} else {
		if this.context.formatDescription.GetChecksumAlg() == binlogevent.BINLOG_CHECKSUM_ALG_CRC32 {
			// fmt.Println("crc32 eventLen:", header.GetEventLen())
			if header.GetEventLen() > 24 {
				endPos := len(by) - 4
				this.Parse(header, NewLogBuffer(by[20:endPos]), this.SwitchLogFile)
			} else {
				fmt.Println("eventType:", header.GetEventType())
			}
		} else {
			// fmt.Printf("notcrc eventLen:%d\t checksumalg:%d\n", header.GetEventLen(), this.context.formatDescription.GetChecksumAlg())
			this.Parse(header, NewLogBuffer(by[20:]), this.SwitchLogFile)
		}
	}
}
func (this *NetBinlogReader) StoreTimePos(t time.Time, fileName string, pos int64) {
	//十秒钟一个记录
	if t.Sub(lastLogTime) >= time.Second*10 {
		str := fmt.Sprintf("时间:%s\t文件名:%s\t位置:%d", t.Format("2006-01-02 15:04:05"), fileName, pos)
		fmt.Println(str)
		lastLogTime = t
	}
}

//读取日志头
func (this *NetBinlogReader) ReadHeader() ([]byte, error) {

	return nil, nil
}

func (this *NetBinlogReader) ReadPacket(eventLen int64) ([]byte, error) {
	return this.readPacket()
}

//切换日志文件
func (this *NetBinlogReader) SwitchLogFile(fileName string, pos int64) error {
	this.binlogFileName = fileName
	fileIndex, _ := strconv.Atoi(strings.Split(fileName, ".")[1])
	this.fileIndex = fileIndex
	return nil
}

func (this *NetBinlogReader) readPacket() ([]byte, error) {
	d, err := this.pkg.ReadPacket()
	this.pkgErr = err
	return d, err
}

func (this *NetBinlogReader) writePacket(data []byte) error {
	err := this.pkg.WritePacket(data)
	this.pkgErr = err
	return err
}

func (this *NetBinlogReader) readInitialHandshake() error {
	data, err := this.readPacket()
	if err != nil {
		seelog.Error(err.Error())
		return err
	}

	if data[0] == ERR_HEADER {
		seelog.Error("read initial handshake error")
		return errors.New("read initial handshake error")
	}

	if data[0] < MinProtocolVersion {
		seelog.Error("invalid protocol version %d, must >= 10", data[0])
		return errors.New("invalid protocol version must >= 10")
	}

	//skip mysql version and connection id
	//mysql version end with 0x00
	//connection id length is 4
	pos := 1 + bytes.IndexByte(data[1:], 0x00) + 1 + 4

	this.salt = append(this.salt, data[pos:pos+8]...)

	//skip filter
	pos += 8 + 1

	//capability lower 2 bytes
	this.capability = uint32(binary.LittleEndian.Uint16(data[pos : pos+2]))

	pos += 2

	if len(data) > pos {
		//skip server charset
		//c.charset = data[pos]
		pos += 1

		this.status = binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		this.capability = uint32(binary.LittleEndian.Uint16(data[pos:pos+2]))<<16 | this.capability

		pos += 2

		//skip auth data len or [00]
		//skip reserved (all [00])
		pos += 10 + 1

		// The documentation is ambiguous about the length.
		// The official Python library uses the fixed length 12
		// mysql-proxy also use 12
		// which is not documented but seems to work.
		this.salt = append(this.salt, data[pos:pos+12]...)
	}

	return nil
}

func (this *NetBinlogReader) writeAuthHandshake() error {
	// Adjust client capability flags based on server support
	capability := CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION |
		CLIENT_LONG_PASSWORD | CLIENT_TRANSACTIONS | CLIENT_LONG_FLAG | CLIENT_MULTI_RESULTS

	capability &= this.capability

	//packet length
	//capbility 4
	//max-packet size 4
	//charset 1
	//reserved all[0] 23
	length := 4 + 4 + 1 + 23

	//username
	length += len(this.user) + 1

	//we only support secure connection
	auth := CalcPassword(this.salt, []byte(this.password))

	length += 1 + len(auth)

	if len(this.db) > 0 {
		capability |= CLIENT_CONNECT_WITH_DB

		length += len(this.db) + 1
	}

	this.capability = capability

	data := make([]byte, length+4)

	//capability [32 bit]
	data[4] = byte(capability)
	data[5] = byte(capability >> 8)
	data[6] = byte(capability >> 16)
	data[7] = byte(capability >> 24)

	//MaxPacketSize [32 bit] (none)
	//data[8] = 0x00
	//data[9] = 0x00
	//data[10] = 0x00
	//data[11] = 0x00

	//Charset [1 byte]
	data[12] = byte(this.collation)

	//Filler [23 bytes] (all 0x00)
	pos := 13 + 23

	//User [null terminated string]
	if len(this.user) > 0 {
		pos += copy(data[pos:], this.user)
	}
	//data[pos] = 0x00
	pos++

	// auth [length encoded integer]
	data[pos] = byte(len(auth))
	pos += 1 + copy(data[pos+1:], auth)

	// db [null terminated string]
	if len(this.db) > 0 {
		pos += copy(data[pos:], this.db)
		//data[pos] = 0x00
	}

	return this.writePacket(data)
}

func (this *NetBinlogReader) writeCommand(command byte) error {
	this.pkg.Sequence = 0

	return this.writePacket([]byte{
		0x01, //1 bytes long
		0x00,
		0x00,
		0x00, //sequence
		command,
	})
}

func (this *NetBinlogReader) writeCommandBuf(command byte, arg []byte) error {
	this.pkg.Sequence = 0

	length := len(arg) + 1

	data := make([]byte, length+4)

	data[4] = command

	copy(data[5:], arg)

	return this.writePacket(data)
}

func (this *NetBinlogReader) writeCommandStr(command byte, arg string) error {
	this.pkg.Sequence = 0

	length := len(arg) + 1

	data := make([]byte, length+4)

	data[4] = command

	copy(data[5:], arg)

	return this.writePacket(data)
}

func (this *NetBinlogReader) writeCommandUint32(command byte, arg uint32) error {
	this.pkg.Sequence = 0

	return this.writePacket([]byte{
		0x05, //5 bytes long
		0x00,
		0x00,
		0x00, //sequence

		command,

		byte(arg),
		byte(arg >> 8),
		byte(arg >> 16),
		byte(arg >> 24),
	})
}

func (this *NetBinlogReader) writeCommandStrStr(command byte, arg1 string, arg2 string) error {
	this.pkg.Sequence = 0

	data := make([]byte, 4, 6+len(arg1)+len(arg2))

	data = append(data, command)
	data = append(data, arg1...)
	data = append(data, 0)
	data = append(data, arg2...)

	return this.writePacket(data)
}

func (this *NetBinlogReader) Ping() error {
	n := time.Now().Unix()

	if n-this.lastPing > pingPeriod {
		if err := this.writeCommand(COM_PING); err != nil {
			return err
		}

		if _, err := this.readOK(); err != nil {
			return err
		}
	}

	this.lastPing = n

	return nil
}

func (this *NetBinlogReader) UseDB(dbName string) error {
	if this.db == dbName {
		return nil
	}

	if err := this.writeCommandStr(COM_INIT_DB, dbName); err != nil {
		return err
	}

	if _, err := this.readOK(); err != nil {
		return err
	}

	this.db = dbName
	return nil
}

func (this *NetBinlogReader) GetDB() string {
	return this.db
}

func (this *NetBinlogReader) Execute(command string, args ...interface{}) (*Result, error) {
	return this.exec(command)
}

func (this *NetBinlogReader) Begin() error {
	_, err := this.exec("begin")
	return err
}

func (this *NetBinlogReader) Commit() error {
	_, err := this.exec("commit")
	return err
}

func (this *NetBinlogReader) Rollback() error {
	_, err := this.exec("rollback")
	return err
}

/*add by chet 2014-12-24 begin*/
func (this *NetBinlogReader) Query(sql string) (*Result, error) {
	return this.exec(sql)
}

/*add end*/

func (this *NetBinlogReader) SetCharset(charset string) error {
	charset = strings.Trim(charset, "\"'`")
	//if c.charset == charset {
	//	return nil
	//}

	cid, ok := CharsetIds[charset]
	if !ok {
		return fmt.Errorf("invalid charset %s", charset)
	}
	collate, ok := Charsets[charset]
	if !ok {
		return fmt.Errorf("invalid charset %s", charset)
	}

	if _, err := this.exec(fmt.Sprintf("set names %s collate %s", charset, collate)); err != nil {
		return err
	} else {
		this.collation = cid
		return nil
	}
}

func (this *NetBinlogReader) FieldList(table string, wildcard string) ([]*Field, error) {
	if err := this.writeCommandStrStr(COM_FIELD_LIST, table, wildcard); err != nil {
		seelog.Error(err.Error())
		return nil, err
	}

	data, err := this.readPacket()
	if err != nil {
		seelog.Error(err.Error())
		return nil, err
	}

	fs := make([]*Field, 0, 4)
	var f *Field
	if data[0] == ERR_HEADER {
		seelog.Error("error header: % #x", data)
		return nil, this.handleErrorPacket(data)
	} else {
		for {
			if data, err = this.readPacket(); err != nil {
				return nil, err
			}

			// EOF Packet
			if this.isEOFPacket(data) {
				return fs, nil
			}

			if f, err = FieldData(data).Parse(); err != nil {
				seelog.Error(err.Error())
				return nil, err
			}
			fs = append(fs, f)
		}
	}
	return nil, fmt.Errorf("field list error")
}

func (this *NetBinlogReader) exec(query string) (*Result, error) {
	if err := this.writeCommandStr(COM_QUERY, query); err != nil {
		seelog.Error(err.Error())
		return nil, err
	}

	return this.readResult(false)
}

func (this *NetBinlogReader) readResultset(data []byte, binary bool) (*Result, error) {
	result := &Result{
		Status:       0,
		InsertId:     0,
		AffectedRows: 0,

		Resultset: &Resultset{},
	}

	// column count
	count, _, n := LengthEncodedInt(data)

	if n-len(data) != 0 {
		seelog.Error(ErrMalformPacket.Error())
		return nil, ErrMalformPacket
	}

	result.Fields = make([]*Field, count)
	result.FieldNames = make(map[string]int, count)

	if err := this.readResultColumns(result); err != nil {
		seelog.Error(err.Error())
		return nil, err
	}

	if err := this.readResultRows(result, binary); err != nil {
		seelog.Error(err.Error())
		return nil, err
	}

	return result, nil
}

func (this *NetBinlogReader) readResultColumns(result *Result) (err error) {
	var i int = 0
	var data []byte

	for {
		data, err = this.readPacket()
		if err != nil {
			seelog.Error(err.Error())
			return
		}

		// EOF Packet
		if this.isEOFPacket(data) {
			if this.capability&CLIENT_PROTOCOL_41 > 0 {
				//result.Warnings = binary.LittleEndian.Uint16(data[1:])
				//todo add strict_mode, warning will be treat as error
				result.Status = binary.LittleEndian.Uint16(data[3:])
				this.status = result.Status
			}

			if i != len(result.Fields) {
				seelog.Error(ErrMalformPacket.Error())
				err = ErrMalformPacket
			}

			return
		}

		result.Fields[i], err = FieldData(data).Parse()
		if err != nil {
			seelog.Error(err.Error())
			return
		}

		result.FieldNames[string(result.Fields[i].Name)] = i

		i++
	}
}

func (this *NetBinlogReader) readResultRows(result *Result, isBinary bool) (err error) {
	var data []byte

	for {
		data, err = this.readPacket()

		if err != nil {
			seelog.Error(err.Error())
			return
		}

		// EOF Packet
		if this.isEOFPacket(data) {
			if this.capability&CLIENT_PROTOCOL_41 > 0 {
				//result.Warnings = binary.LittleEndian.Uint16(data[1:])
				//todo add strict_mode, warning will be treat as error
				result.Status = binary.LittleEndian.Uint16(data[3:])
				this.status = result.Status
			}

			break
		}

		result.RowDatas = append(result.RowDatas, data)
	}

	result.Values = make([][]interface{}, len(result.RowDatas))
	//add by chet
	result.AffectedRows = uint64(len(result.RowDatas))
	//end
	for i := range result.Values {
		result.Values[i], err = result.RowDatas[i].Parse(result.Fields, isBinary)

		if err != nil {
			seelog.Error(err.Error())
			return err
		}
	}

	return nil
}

func (this *NetBinlogReader) readUntilEOF() (err error) {
	var data []byte

	for {
		data, err = this.readPacket()

		if err != nil {
			seelog.Error(err.Error())
			return
		}

		// EOF Packet
		if this.isEOFPacket(data) {
			return
		}
	}
	return
}

func (this *NetBinlogReader) isEOFPacket(data []byte) bool {
	return data[0] == EOF_HEADER && len(data) <= 5
}

func (this *NetBinlogReader) handleOKPacket(data []byte) (*Result, error) {
	var n int
	var pos int = 1

	r := new(Result)

	r.AffectedRows, _, n = LengthEncodedInt(data[pos:])
	pos += n
	r.InsertId, _, n = LengthEncodedInt(data[pos:])
	pos += n

	if this.capability&CLIENT_PROTOCOL_41 > 0 {
		r.Status = binary.LittleEndian.Uint16(data[pos:])
		this.status = r.Status
		pos += 2

		//todo:strict_mode, check warnings as error
		//Warnings := binary.LittleEndian.Uint16(data[pos:])
		//pos += 2
	} else if this.capability&CLIENT_TRANSACTIONS > 0 {
		r.Status = binary.LittleEndian.Uint16(data[pos:])
		this.status = r.Status
		pos += 2
	}

	//info
	return r, nil
}

func (this *NetBinlogReader) handleErrorPacket(data []byte) error {
	e := new(SqlError)

	var pos int = 1

	e.Code = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if this.capability&CLIENT_PROTOCOL_41 > 0 {
		//skip '#'
		pos++
		e.State = string(data[pos : pos+5])
		pos += 5
	}

	e.Message = string(data[pos:])

	return e
}

func (this *NetBinlogReader) readOK() (*Result, error) {
	data, err := this.readPacket()
	if err != nil {
		seelog.Error(err.Error())
		return nil, err
	}

	if data[0] == OK_HEADER {
		return this.handleOKPacket(data)
	} else if data[0] == ERR_HEADER {
		return nil, this.handleErrorPacket(data)
	} else {
		return nil, errors.New("invalid ok packet")
	}
}

func (this *NetBinlogReader) readResult(binary bool) (*Result, error) {
	data, err := this.readPacket()
	if err != nil {
		seelog.Error(err.Error())
		return nil, err
	}

	if data[0] == OK_HEADER {
		return this.handleOKPacket(data)
	} else if data[0] == ERR_HEADER {
		return nil, this.handleErrorPacket(data)
	} else if data[0] == LocalInFile_HEADER {
		return nil, ErrMalformPacket
	}

	return this.readResultset(data, binary)
}

func (this *NetBinlogReader) IsAutoCommit() bool {
	return this.status&SERVER_STATUS_AUTOCOMMIT > 0
}

func (this *NetBinlogReader) IsInTransaction() bool {
	return this.status&SERVER_STATUS_IN_TRANS > 0
}

func (this *NetBinlogReader) GetCharset() string {
	return this.charset
}

func (this *NetBinlogReader) SetTableMetaCache(tableMetaCache *TableMetaCache) {
	this.tableMetaCache = tableMetaCache
}
