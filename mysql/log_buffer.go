package mysql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	DIG_PER_INT32 = 9
	SIZE_OF_INT32 = 4
	DIG_PER_DEC1  = 9
)

var (
	dig2bytes = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}
	powers10  = []uint32{1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000}
)

type LogBuffer struct {
	buffer []byte
	pos    int
	length int
}

func NewLogBuffer(buffer []byte) *LogBuffer {
	this := new(LogBuffer)
	this.buffer = buffer
	this.pos = 0
	this.length = len(buffer)
	return this
}

func (this *LogBuffer) HasMore() bool {
	return this.pos < this.length
}

func (this *LogBuffer) GetByte() byte {
	bytebuf := this.buffer[this.pos]
	this.pos++
	return bytebuf
}

func (this *LogBuffer) GetInt8() int {
	buf := this.buffer[this.pos : this.pos+1]
	this.pos++
	return int(buf[0])
}

func (this *LogBuffer) GetUInt8() int {
	// buf := this.buffer[this.pos : this.pos+1]
	result := int(0xff & this.buffer[this.pos])
	this.pos++
	return result
}

func (this *LogBuffer) GetInt16() int {
	buf := this.buffer[this.pos : this.pos+2]
	this.pos += 2
	return int(0xff&buf[0]) | int(buf[1])<<8
}

func (this *LogBuffer) GetBeInt16() int {
	buf := this.buffer[this.pos : this.pos+2]
	this.pos += 2
	return int(0xff&buf[1]) | int(buf[0])<<8
}

func (this *LogBuffer) GetUInt16() int {
	buf := this.buffer[this.pos : this.pos+2]
	this.pos += 2
	return int(binary.LittleEndian.Uint16(buf[0:2]))
}

func (this *LogBuffer) GetBeUInt16() int {
	buf := this.buffer[this.pos : this.pos+2]
	this.pos += 2
	return int(binary.BigEndian.Uint16(buf[0:2]))
}

func (this *LogBuffer) GetInt24() int {
	buf := this.buffer[this.pos : this.pos+3]
	this.pos += 3
	return int(0xff&buf[0]) | int(0xff&buf[1])<<8 | int(buf[2])<<16
}

func (this *LogBuffer) GetBeInt24() int {
	buf := this.buffer[this.pos : this.pos+3]
	this.pos += 3
	return int(0xff&buf[2]) | int(0xff&buf[1])<<8 | int(buf[0])<<16
}

func (this *LogBuffer) GetUInt24() int {
	buf := this.buffer[this.pos : this.pos+3]
	this.pos += 3
	return int(0xff&buf[0]) | int(0xff&buf[1])<<8 | int(0xff&buf[2])<<16
}

func (this *LogBuffer) GetBeUInt24() int {
	buf := this.buffer[this.pos : this.pos+3]
	this.pos += 3
	return int(0xff&buf[2]) | int(0xff&buf[1])<<8 | int(0xff&buf[0])<<16
}

func (this *LogBuffer) GetInt32() int32 {
	buf := this.buffer[this.pos : this.pos+4]
	this.pos += 4
	return int32(0xff&buf[0]) | int32(0xff&buf[1])<<8 | int32(0xff&buf[2])<<16 | int32(buf[3])<<24
}

func (this *LogBuffer) GetBeInt32() int32 {
	buf := this.buffer[this.pos : this.pos+4]
	this.pos += 4
	return int32(0xff&buf[3]) | int32(0xff&buf[2])<<8 | int32(0xff&buf[1])<<16 | int32(buf[0])<<24
}

func (this *LogBuffer) GetUInt32() uint32 {
	buf := this.buffer[this.pos : this.pos+4]
	this.pos += 4
	return binary.LittleEndian.Uint32(buf[0:4])
}

func (this *LogBuffer) GetBeUInt32() uint32 {
	buf := this.buffer[this.pos : this.pos+4]
	this.pos += 4
	return binary.BigEndian.Uint32(buf[0:4])
}

func (this *LogBuffer) GetUInt40() int64 {
	buf := this.buffer[this.pos : this.pos+5]
	this.pos += 5
	return int64(0xff&buf[0]) | int64(0xff&buf[1])<<8 | int64(0xff&buf[2])<<16 | int64(0xff&buf[3])<<24 | int64(0xff&buf[4])<<32
}

func (this *LogBuffer) GetBeUInt40() int64 {
	buf := this.buffer[this.pos : this.pos+5]
	this.pos += 5
	return int64(0xff&buf[4]) | int64(0xff&buf[3])<<8 | int64(0xff&buf[2])<<16 | int64(0xff&buf[1])<<24 | int64(0xff&buf[0])<<32
}

func (this *LogBuffer) GetInt48() int64 {
	buf := this.buffer[this.pos : this.pos+6]
	this.pos += 6
	return int64(0xff&buf[0]) | int64(0xff&buf[1])<<8 | int64(0xff&buf[2])<<16 | int64(0xff&buf[3])<<24 | int64(0xff&buf[4])<<32 | int64(buf[5])<<40
}

func (this *LogBuffer) GetBeInt48() int64 {
	buf := this.buffer[this.pos : this.pos+6]
	this.pos += 6
	return int64(0xff&buf[5]) | int64(0xff&buf[4])<<8 | int64(0xff&buf[3])<<16 | int64(0xff&buf[2])<<24 | int64(0xff&buf[1])<<32 | int64(buf[0])<<40
}

func (this *LogBuffer) GetUInt48() int64 {
	buf := this.buffer[this.pos : this.pos+6]
	this.pos += 6
	return int64(0xff&buf[0]) | int64(0xff&buf[1])<<8 | int64(0xff&buf[2])<<16 | int64(0xff&buf[3])<<24 | int64(0xff&buf[4])<<32 | int64(0xff&buf[5])<<40
}

func (this *LogBuffer) GetBeUInt48() int64 {
	buf := this.buffer[this.pos : this.pos+6]
	this.pos += 6
	return int64(0xff&buf[5]) | int64(0xff&buf[4])<<8 | int64(0xff&buf[3])<<16 | int64(0xff&buf[2])<<24 | int64(0xff&buf[1])<<32 | int64(0xff&buf[0])<<40
}

func (this *LogBuffer) GetUInt56() int64 {
	buf := this.buffer[this.pos : this.pos+7]
	this.pos += 7
	return int64(0xff&buf[0]) | int64(0xff&buf[1])<<8 | int64(0xff&buf[2])<<16 | int64(0xff&buf[3])<<24 | int64(0xff&buf[4])<<32 | int64(0xff&buf[5])<<40 | int64(0xff&buf[6])<<48
}

func (this *LogBuffer) GetBeUInt56() int64 {
	buf := this.buffer[this.pos : this.pos+7]
	this.pos += 7
	return int64(0xff&buf[6]) | int64(0xff&buf[5])<<8 | int64(0xff&buf[4])<<16 | int64(0xff&buf[3])<<24 | int64(0xff&buf[2])<<32 | int64(0xff&buf[1])<<40 | int64(0xff&buf[6])<<48
}

func (this *LogBuffer) GetInt64() int64 {
	buf := this.buffer[this.pos : this.pos+8]
	this.pos += 8
	return int64(0xff&buf[0]) | int64(0xff&buf[1])<<8 | int64(0xff&buf[2])<<16 | int64(0xff&buf[3])<<24 | int64(0xff&buf[4])<<32 | int64(0xff&buf[5])<<40 | int64(0xff&buf[6])<<48 | int64(buf[7])<<56
}

func (this *LogBuffer) GetBeInt64() int64 {
	buf := this.buffer[this.pos : this.pos+8]
	this.pos += 8
	return int64(0xff&buf[7]) | int64(0xff&buf[6])<<8 | int64(0xff&buf[5])<<16 | int64(0xff&buf[4])<<24 | int64(0xff&buf[3])<<32 | int64(0xff&buf[2])<<40 | int64(0xff&buf[1])<<48 | int64(buf[0])<<56
}

func (this *LogBuffer) GetUInt64() uint64 {
	buf := this.buffer[this.pos : this.pos+8]
	this.pos += 8
	return binary.LittleEndian.Uint64(buf[0:8])
}

func (this *LogBuffer) GetBeUInt64() uint64 {
	buf := this.buffer[this.pos : this.pos+8]
	this.pos += 8
	return binary.BigEndian.Uint64(buf[0:8])
}

func (this *LogBuffer) GetFloat32() string {
	buf := this.buffer[this.pos : this.pos+4]
	this.pos += 4
	return strconv.FormatFloat(float64(math.Float32frombits(binary.LittleEndian.Uint32(buf[0:4]))), 'f', 6, 64)
}

func (this *LogBuffer) GetFloat64() string {
	buf := this.buffer[this.pos : this.pos+8]
	this.pos += 8
	return strconv.FormatFloat(math.Float64frombits(binary.LittleEndian.Uint64(buf[0:8])), 'f', 6, 64)
}

func (this *LogBuffer) GetServerVersion() string {
	buf := this.buffer[this.pos : this.pos+50]
	for i := 0; i < len(buf); i++ {
		if buf[i] == 0x00 {
			this.pos += 50
			return string(buf[:i])
		}
	}
	this.pos += 50
	return ""
}

func (this *LogBuffer) GetRestString() string {
	buf := this.buffer[this.pos:]
	this.pos += len(buf)
	return string(buf)
}

func (this *LogBuffer) GetRestBytes() []byte {
	buf := this.buffer[this.pos:]
	this.pos += len(buf)
	return buf
}

func (this *LogBuffer) GetRestLen() int {
	return this.length - this.pos
}

func (this *LogBuffer) GetVarLenString(length int) string {
	buf := this.buffer[this.pos : this.pos+length]
	this.pos += length
	return string(buf)
}

func (this *LogBuffer) GetVarLenBytes(length int) []byte {
	buf := this.buffer[this.pos : this.pos+length]
	this.pos += length
	return buf
}

func (this *LogBuffer) GetVarLen() (uint64, error) {
	byte0 := this.GetByte()
	if byte0 < 0xfb {
		return uint64(byte0), nil
	} else if byte0 == 0xfb {
		return uint64(0), errors.New("end of packet")
	} else if byte0 == 0xfc {
		return uint64(this.GetUInt16()), nil
	} else if byte0 == 0xfd {
		return uint64(this.GetUInt24()), nil
	} else if byte0 == 0xfe {
		return this.GetUInt64(), nil
	} else {
		return uint64(0), errors.New("end of packet")
	}

}

func (this *LogBuffer) GetLengthEnodedString() ([]byte, error) {

	length, _ := this.GetVarLen()

	if length < 1 {
		return nil, errors.New("length is small then 1")
	} else {
		str := this.buffer[this.pos : this.pos+int(length)]
		this.pos += int(length)
		return str, nil
	}
}

func (this *LogBuffer) GetRestLength() int {
	return this.length - this.pos
}

func (this *LogBuffer) SkipLen(length int) {
	this.pos += length
}

func (this *LogBuffer) Position(pos int) {
	this.pos = pos
}

func (this *LogBuffer) GetLength() int {
	return this.length
}

func (this *LogBuffer) GetDecimal(precision, scale int) (string, int) {
	by := this.buffer[this.pos:]

	intg := precision - scale
	frac := scale
	intg0 := intg / DIG_PER_INT32
	frac0 := frac / DIG_PER_INT32
	intg0x := intg - intg0*DIG_PER_INT32
	frac0x := frac - frac0*DIG_PER_INT32

	binSize := intg0*SIZE_OF_INT32 + dig2bytes[intg0x] + frac0*SIZE_OF_INT32 + dig2bytes[frac0x]
	// if this.pos+binSize > this.pos+this.length {
	// 	// panic(fmt.Sprintf("limit excceed:%d" + (position + binSize - origin)))
	// 	panic("ddd")
	// }

	// fmt.Printf("bytes:[% 2x] intg:%d\t frac:%d\t intg0:%d\t frac0:%d\t intg0x:%d\t frac0x:%d\t binSize:%d\t\n",
	// by, intg, frac, intg0, frac0, intg0x, frac0x, binSize)
	str := this.getDecimal(by, intg, frac, intg0, frac0, intg0x, frac0x)
	this.pos += binSize
	return str, binSize
}

func (this *LogBuffer) getDecimal(by []byte, intg, frac, intg0, frac0, intg0x, frac0x int) string {
	var mask, length, from int

	if (by[0] & 0x80) == 0x80 {
		mask = 0
	} else {
		mask = -1
	}

	if mask != 0 {
		length = 1
	} else {
		length = 0
	}

	if intg != 0 {
		length += intg
	} else {
		length += 1
	}

	if frac != 0 {
		length += 1
	}

	length += frac

	buf := make([]byte, length)

	pos := int(0)
	// 添加负号
	if mask != 0 {
		buf[pos] = '-'
		pos++
	}

	by[0] ^= 0x80
	mark := pos

	if intg0x != 0 {
		i := dig2bytes[intg0x]
		var x int32 = 0
		switch i {
		case 1:
			x = int32(int8(by[from] & 0xff))
		case 2:
			x = int32(binary.BigEndian.Uint16(by[from : from+2]))
		case 3:
			x = int32(by[from])<<16 | int32(0xff&by[from+1])<<8 | int32(0xff&by[from+2])
		case 4:
			x = int32(binary.BigEndian.Uint32(by[from : from+4]))
		}

		from += i
		x ^= int32(mask)

		if x != 0 {
			for j := intg0x; j > 0; j-- {
				divisor := int32(powers10[j-1])
				y := x / divisor
				if mark < pos || y != 0 {
					buf[pos] = byte('0' + y)
					pos++
				}
				x -= y * divisor
			}
		}
	}

	for stop := from + intg0*SIZE_OF_INT32; from < stop; from += SIZE_OF_INT32 {
		x := int32(binary.BigEndian.Uint32(by[from : from+4]))
		x ^= int32(mask)
		if x != 0 {
			if mark < pos {
				for i := DIG_PER_DEC1; i > 0; i-- {
					divisor := int32(powers10[i-1])
					y := x / int32(divisor)
					buf[pos] = byte('0' + y)
					pos++
					x -= y * divisor
				}
			} else {
				for i := DIG_PER_DEC1; i > 0; i-- {
					divisor := int32(powers10[i-1])
					y := x / int32(divisor)
					if mark < pos || y != 0 {
						buf[pos] = byte('0' + y)
						pos++
					}
					x -= y * divisor
				}
			}
		} else if mark < pos {
			for i := DIG_PER_DEC1; i > 0; i-- {
				buf[pos] = '0'
				pos++
			}
		}

	}

	if mark == pos {
		buf[pos] = '0'
		pos++
	}

	if frac > 0 {
		buf[pos] = '.'
		pos++
		mark = pos

		for stop := from + frac0*SIZE_OF_INT32; from < stop; from += SIZE_OF_INT32 {
			x := int32(binary.BigEndian.Uint32(by[from : from+4]))
			x ^= int32(mask)
			if x != 0 {
				for i := DIG_PER_DEC1; i > 0; i-- {
					divisor := int32(powers10[i-1])
					y := x / divisor
					buf[pos] = byte('0' + y)
					pos++
					x -= y * divisor
				}
			} else {
				for i := DIG_PER_DEC1; i > 0; i-- {
					buf[pos] = '0'
					pos++
				}
			}
		}

		if frac0x != 0 {
			i := dig2bytes[frac0x]
			var x int32 = 0
			switch i {
			case 1:
				x = int32(int8(by[from] & 0xff))
			case 2:
				x = int32(binary.BigEndian.Uint16(by[from : from+2]))
			case 3:
				x = int32(by[from])<<16 | int32(by[from+1])<<8 | int32(by[from+2])
			case 4:
				x = int32(binary.BigEndian.Uint32(by[from : from+4]))
			}

			x ^= int32(mask)

			if x != 0 {
				dig := DIG_PER_DEC1 - frac0x
				x *= int32(powers10[dig])

				for j := DIG_PER_DEC1; j > dig; j-- {
					divisor := int32(powers10[j-1])
					y := x / divisor
					buf[pos] = byte('0' + y)
					pos++
					x -= y * divisor
				}
			}
		}

		if mark == pos {
			buf[pos] = '0'
			pos++
		}
	}

	by[0] ^= 0x80

	buf = bytes.TrimRight(buf, string([]byte{0}))

	return string(buf)
}

func (this *LogBuffer) GetTimeStringFromUnixTimeStamp(timestamp int64) string {
	timeStr := time.Unix(timestamp, 0).Format(time.RFC3339)
	timeStr = strings.Replace(timeStr, "T", " ", 1)
	timeStr = strings.Split(timeStr, "+")[0]
	return timeStr
}

func (this *LogBuffer) GetPosition() int {
	return this.pos
}
