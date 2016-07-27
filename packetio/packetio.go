package packetio

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/golang/protobuf/proto"
	"io"
	"github.com/SDHM/sqlregret/protocol"
	"net"
)

type PacketIO struct {
	rb *bufio.Reader
	wb io.Writer
}

var (
	ErrBadConn       = errors.New("connection was bad")
	ErrMalformPacket = errors.New("Malform packet error")

	ErrTxDone = errors.New("sql: Transaction has already been committed or rolled back")
)

func NewPacketIO(conn net.Conn) *PacketIO {
	p := new(PacketIO)

	p.rb = bufio.NewReaderSize(conn, 1024)
	p.wb = conn

	return p
}

func (p *PacketIO) ReadPacket() (*protocol.Packet, error) {

	header := []byte{0, 0, 0, 0}

	if _, err := io.ReadFull(p.rb, header); err != nil {
		return nil, ErrBadConn
	}

	length := binary.BigEndian.Uint32(header[0:])
	body := make([]byte, length)

	if _, err := io.ReadFull(p.rb, body); err != nil {
		panic(err)
		return nil, ErrBadConn
	}

	pkt := &protocol.Packet{}
	if err := proto.Unmarshal(body, pkt); nil != err {
		return nil, err
	}

	return pkt, nil

}

func (p *PacketIO) WritePacket(pkt *protocol.Packet) error {

	buf, err := proto.Marshal(pkt)
	if nil != err {
		return err
	}

	length := int32(len(buf))
	lenbuf := bytes.NewBuffer([]byte{})
	binary.Write(lenbuf, binary.BigEndian, length)

	buf1 := make([]byte, 0, length+4)

	buf1 = append(buf1, lenbuf.Bytes()...)
	buf1 = append(buf1, buf...)

	if n, err := p.wb.Write(buf1); err != nil {
		return ErrBadConn
	} else if n != len(buf1) {
		return ErrBadConn
	}

	return nil
}
