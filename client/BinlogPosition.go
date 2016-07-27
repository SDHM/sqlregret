package client

type BinlogPosition struct {
	fileName string
	position int64
}

func NewBinlogPosition(fileName string, position int64) *BinlogPosition {
	this := new(BinlogPosition)
	this.fileName = fileName
	this.position = position
	return this
}

func (this *BinlogPosition) GetFileName() string {
	return this.fileName
}

func (this *BinlogPosition) GetPosition() int64 {
	return this.position
}
