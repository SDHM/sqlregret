package client

import (
	"strings"
)

type TableMeta struct {
	FullName string
	Fileds   []*FieldMeta
}

type FieldMeta struct {
	ColumnName   string
	ColumnType   string
	IsNullable   string
	IsKey        string
	DefaultValue string
	Extra        string
}

func NewTableMeta(fullName string, fields []*FieldMeta) *TableMeta {
	this := new(TableMeta)
	this.FullName = fullName
	this.Fileds = fields
	return this
}

func (this *FieldMeta) IsThisKey() bool {
	return strings.EqualFold(this.IsKey, "PRI")
}

func (this *FieldMeta) IsThisNullable() bool {
	return strings.EqualFold(this.IsNullable, "YES")
}

func (this *FieldMeta) IsThisUnsigned() bool {
	return strings.ContainsAny(this.ColumnType, "unsigned")
}

func (this *FieldMeta) IsThisText() bool {
	return strings.EqualFold("LONGTEXT", this.ColumnType) || strings.EqualFold("MEDIUMTEXT", this.ColumnType) || strings.EqualFold("TEXT", this.ColumnType) || strings.EqualFold("TINYTEXT", this.ColumnType)
}

func (this *FieldMeta) IsBinary() bool {
	isBinary := false
	isBinary = strings.ContainsAny(this.ColumnType, "VARBINARY") || strings.ContainsAny(this.ColumnType, "BINARY")
	return isBinary
}

func (this *FieldMeta) String() string {
	return "FieldMeta [columnName=" + this.ColumnName + ", columnType=" + this.ColumnType + ", defaultValue=" + this.DefaultValue + ", extra=" + this.Extra + ", isNullable=" + this.IsNullable + ", iskey=" + this.IsKey + "]"
}
