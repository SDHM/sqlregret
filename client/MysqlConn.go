package client

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	. "github.com/SDHM/sqlregret/mysql"
	"github.com/cihub/seelog"
)

var (
	pingPeriod = int64(time.Second * 30)
)

//proxy <-> mysql server
type MysqlConnection struct {
	conn net.Conn

	pkg *PacketIO

	addr     string
	user     string
	password string
	db       string

	self_name     string
	self_user     string
	self_password string
	self_port     uint16
	self_slaveId  uint32

	capability uint32

	status uint16

	collation CollationId
	charset   string
	salt      []byte

	lastPing int64

	tableMetaCache *TableMetaCache
	context        *LogContext
	logger         *log.Logger
	binlogFileName string
	pkgErr         error
}

func NewMysqlConnection(
	masterAddr, user, password, dbName string,
	port uint16,
	slaveId uint32) *MysqlConnection {
	this := new(MysqlConnection)
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

	return this
}

func (this *MysqlConnection) Connect() error {
	return this.ReConnect()
}

func (this *MysqlConnection) ReConnect() error {
	if this.conn != nil {
		this.conn.Close()
	}

	n := "tcp"
	if strings.Contains(this.addr, "/") {
		n = "unix"
	}
	fmt.Println("begin connect server !")
	netConn, err := net.Dial(n, this.addr)
	if err != nil {
		seelog.Error(err.Error())
		return err
	}
	fmt.Println("Connect server succeed!")
	this.conn = netConn
	this.pkg = NewPacketIO(netConn)

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

func (this *MysqlConnection) Close() error {
	if this.conn != nil {
		this.conn.Close()
		this.conn = nil
	}

	return nil
}

func (this *MysqlConnection) readPacket() ([]byte, error) {
	d, err := this.pkg.ReadPacket()
	this.pkgErr = err
	return d, err
}

func (this *MysqlConnection) writePacket(data []byte) error {
	err := this.pkg.WritePacket(data)
	this.pkgErr = err
	return err
}

func (this *MysqlConnection) readInitialHandshake() error {
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

func (this *MysqlConnection) writeAuthHandshake() error {
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

func (this *MysqlConnection) writeCommand(command byte) error {
	this.pkg.Sequence = 0

	return this.writePacket([]byte{
		0x01, //1 bytes long
		0x00,
		0x00,
		0x00, //sequence
		command,
	})
}

func (this *MysqlConnection) writeCommandBuf(command byte, arg []byte) error {
	this.pkg.Sequence = 0

	length := len(arg) + 1

	data := make([]byte, length+4)

	data[4] = command

	copy(data[5:], arg)

	return this.writePacket(data)
}

func (this *MysqlConnection) writeCommandStr(command byte, arg string) error {
	this.pkg.Sequence = 0

	length := len(arg) + 1

	data := make([]byte, length+4)

	data[4] = command

	copy(data[5:], arg)

	return this.writePacket(data)
}

func (this *MysqlConnection) writeCommandUint32(command byte, arg uint32) error {
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

func (this *MysqlConnection) writeCommandStrStr(command byte, arg1 string, arg2 string) error {
	this.pkg.Sequence = 0

	data := make([]byte, 4, 6+len(arg1)+len(arg2))

	data = append(data, command)
	data = append(data, arg1...)
	data = append(data, 0)
	data = append(data, arg2...)

	return this.writePacket(data)
}

func (this *MysqlConnection) Ping() error {
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

func (this *MysqlConnection) UseDB(dbName string) error {
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

func (this *MysqlConnection) GetDB() string {
	return this.db
}

func (this *MysqlConnection) Execute(command string, args ...interface{}) (*Result, error) {
	return this.exec(command)
}

func (this *MysqlConnection) Begin() error {
	_, err := this.exec("begin")
	return err
}

func (this *MysqlConnection) Commit() error {
	_, err := this.exec("commit")
	return err
}

func (this *MysqlConnection) Rollback() error {
	_, err := this.exec("rollback")
	return err
}

/*add by chet 2014-12-24 begin*/
func (this *MysqlConnection) Query(sql string) (*Result, error) {
	return this.exec(sql)
}

/*add end*/

func (this *MysqlConnection) SetCharset(charset string) error {
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

func (this *MysqlConnection) FieldList(table string, wildcard string) ([]*Field, error) {
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

func (this *MysqlConnection) exec(query string) (*Result, error) {
	if err := this.writeCommandStr(COM_QUERY, query); err != nil {
		seelog.Error(err.Error())
		return nil, err
	}

	return this.readResult(false)
}

func (this *MysqlConnection) readResultset(data []byte, binary bool) (*Result, error) {
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

func (this *MysqlConnection) readResultColumns(result *Result) (err error) {
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

func (this *MysqlConnection) readResultRows(result *Result, isBinary bool) (err error) {
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

func (this *MysqlConnection) readUntilEOF() (err error) {
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

func (this *MysqlConnection) isEOFPacket(data []byte) bool {
	return data[0] == EOF_HEADER && len(data) <= 5
}

func (this *MysqlConnection) handleOKPacket(data []byte) (*Result, error) {
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

func (this *MysqlConnection) handleErrorPacket(data []byte) error {
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

func (this *MysqlConnection) readOK() (*Result, error) {
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

func (this *MysqlConnection) readResult(binary bool) (*Result, error) {
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

func (this *MysqlConnection) IsAutoCommit() bool {
	return this.status&SERVER_STATUS_AUTOCOMMIT > 0
}

func (this *MysqlConnection) IsInTransaction() bool {
	return this.status&SERVER_STATUS_IN_TRANS > 0
}

func (this *MysqlConnection) GetCharset() string {
	return this.charset
}

func (this *MysqlConnection) SetTableMetaCache(tableMetaCache *TableMetaCache) {
	this.tableMetaCache = tableMetaCache
}
