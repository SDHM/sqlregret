package mysql

const (
	UNKNOWN_EVENT            int    = iota
	START_EVENT_V3                  //0x01	A start event is the first event of a binlog for binlog-version 1 to 3
	QUERY_EVENT                     //0x02	事物开始 BEGIN事件 binlog_format='STATEMENT' ,具体执行的语句保存在QUERY_EVENT事件中 对于ROW 格式的BINLOG,所有DDL以文本格式的记录在QUERY_EVENT中
	STOP_EVENT                      //0x03
	ROTATE_EVENT                    //0x04
	INTVAR_EVENT                    //0x05  Integer based session-variables
	LOAD_EVENT                      //0x06
	SLAVE_EVENT                     //0x07
	CREATE_FILE_EVENT               //0x08
	APPEND_BLOCK_EVENT              //0x09
	EXEC_LOAD_EVENT                 //0x0a
	DELETE_FILE_EVENT               //0x0b
	NEW_LOAD_EVENT                  //0x0c
	RAND_EVENT                      //0x0d
	USER_VAR_EVENT                  //0x0e
	FORMAT_DESCRIPTION_EVENT        //0x0f	MYSQL根据其定义的来解析其他事件
	XID_EVENT                       //0x10	事务提交(MYSQL进行崩溃恢复时间,根据事务在binlog中的提交情况来决定是否提交存储引擎中状态为prepared的事物)
	BEGIN_LOAD_QUERY_EVENT          //0x11
	EXECUTE_LOAD_QUERY_EVENT        //0x12
	TABLE_MAP_EVENT                 //0x13	每个ROWS_EVENT事件之前有一个TABLE_MAP_EVENT用于描述内部ID和结构定义
	WRITE_ROWS_EVENTv0              //0x14	包含了要插入的数据
	UPDATE_ROWS_EVENTv0             //0x15	包含了行修改前的值,也包含了修改后的值
	DELETE_ROWS_EVENTv0             //0x16
	WRITE_ROWS_EVENTv1              //0x17	包含了要插入的数据
	UPDATE_ROWS_EVENTv1             //0x18	包含了行修改前的值,也包含了修改后的值
	DELETE_ROWS_EVENTv1             //0x19	包含了需要删除行的主键值/行号
	INCIDENT_EVENT                  //0x1a
	HEARTBEAT_EVENT                 //0x1b
	IGNORABLE_EVENT                 //0x1c
	ROWS_QUERY_EVENT                //0x1d
	WRITE_ROWS_EVENTv2              //0x1e	包含了要插入的数据
	UPDATE_ROWS_EVENTv2             //0x1f	包含了行修改前的值,也包含了修改后的值
	DELETE_ROWS_EVENTv2             //0x20	包含了需要删除行的主键值/行号
	GTID_LOG_EVENT                  //0x21
	ANONYMOUS_GTID_EVENT            //0x22
	PREVIOUS_GTIDS_EVENT            //0x23
	ANNOTATE_ROWS_EVENT      = 0xa0 //160
	BINLOG_CHECKPOINT_EVENT  = 0xa1 //161
	GTID_EVENT               = 0xa2 //162
	GTID_LIST_EVENT          = 0xa3 //163
)
