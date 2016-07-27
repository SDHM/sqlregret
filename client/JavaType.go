package client

type JavaType int32

const (
	BIT                     JavaType = -7
	TINYINT                 JavaType = -6
	SMALLINT                JavaType = 5
	INTEGER                 JavaType = 4
	BIGINT                  JavaType = -5
	FLOAT                   JavaType = 6
	REAL                    JavaType = 7
	DOUBLE                  JavaType = 8
	NUMERIC                 JavaType = 2
	DECIMAL                 JavaType = 3
	CHAR                    JavaType = 1
	VARCHAR                 JavaType = 12
	LONGVARCHAR             JavaType = -1
	DATE                    JavaType = 91
	TIME                    JavaType = 92
	TIMESTAMP               JavaType = 93
	BINARY                  JavaType = -2
	VARBINARY               JavaType = -3
	LONGVARBINARY           JavaType = -4
	NULL                    JavaType = 0
	OTHER                   JavaType = 1111
	JAVA_OBJECT             JavaType = 2000
	DISTINCT                JavaType = 2001
	STRUCT                  JavaType = 2002
	ARRAY                   JavaType = 2003
	BLOB                    JavaType = 2004
	CLOB                    JavaType = 2005
	REF                     JavaType = 2006
	DATALINK                JavaType = 70
	BOOLEAN                 JavaType = 16
	ROWID                   JavaType = -8
	NCHAR                   JavaType = -15
	NVARCHAR                JavaType = -9
	LONGNVARCHAR            JavaType = -16
	NCLOB                   JavaType = 2011
	SQLXML                  JavaType = 2009
	REF_CURSOR              JavaType = 2012
	TIME_WITH_TIMEZONE      JavaType = 2013
	TIMESTAMP_WITH_TIMEZONE JavaType = 2014
)
