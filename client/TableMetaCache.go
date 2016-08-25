package client

import (
	"github.com/SDHM/sqlregret/mysql"
)

type TableMetaCache struct {
	reader            IBinlogReader
	tableMetaCacheMap map[string]*TableMeta
}

func NewTableMetaCache(reader IBinlogReader) *TableMetaCache {
	this := new(TableMetaCache)
	this.reader = reader
	this.tableMetaCacheMap = make(map[string]*TableMeta, 10)
	return this
}

func (this *TableMetaCache) getTableMeta(fullName string, flush bool) *TableMeta {

	if flush {
		if rst, err := this.reader.Query("desc " + fullName); nil != err {
			return nil
		} else {
			this.tableMetaCacheMap[fullName] = this.parserTableMeta(rst, fullName)
			return this.tableMetaCacheMap[fullName]
		}
	}

	v, ok := this.tableMetaCacheMap[fullName]
	if !ok {
		if rst, err := this.reader.Query("desc " + fullName); nil != err {
			return nil
		} else {
			this.tableMetaCacheMap[fullName] = this.parserTableMeta(rst, fullName)
			return this.tableMetaCacheMap[fullName]
		}
	} else {
		return v
	}
}

func (this *TableMetaCache) parserTableMeta(rst *mysql.Result, fullName string) *TableMeta {

	fieldMetas := make([]*FieldMeta, 0)
	for index := range rst.Values {
		fieldMeta := new(FieldMeta)
		var result string
		for index2 := range rst.Values[index] {
			switch value := rst.Values[index][index2].(type) {
			case []uint8:
				result = string(value)
			default:
				result = ""
			}

			if index2 == 0 {
				fieldMeta.ColumnName = result
			} else if index2 == 1 {
				fieldMeta.ColumnType = result
			} else if index2 == 2 {
				fieldMeta.IsNullable = result
			} else if index2 == 3 {
				fieldMeta.IsKey = result
			} else if index2 == 4 {
				fieldMeta.DefaultValue = result
			} else if index2 == 5 {
				fieldMeta.Extra = result
			}
		}
		fieldMetas = append(fieldMetas, fieldMeta)
	}

	return NewTableMeta(fullName, fieldMetas)
}
