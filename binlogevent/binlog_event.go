package binlogevent

type Enum_Post_Header_len int

type Log_Event_Type int

const (
	LOG_EVENT_HEADER_LEN = 19 /* the fixed header length */
	OLD_HEADER_LEN       = 13 /* the fixed header length in 3.23 */
	LOG_EVENT_TYPES      = (ENUM_END_EVENT - 1)
	ST_SERVER_VER_LEN    = 50
)

const (
	UNKNOWN_EVENT             Log_Event_Type = 0 // Every time you update this enum (when you add a type), you have to fix Format_description_event::Format_description_event().
	START_EVENT_V3                           = 1 // A start event is the first event of a binlog for binlog-version 1 to 3
	QUERY_EVENT                              = 2 // 事物开始 BEGIN事件 binlog_format='STATEMENT' ,具体执行的语句保存在QUERY_EVENT事件中 对于ROW 格式的BINLOG,所有DDL以文本格式的记录在QUERY_EVENT中
	STOP_EVENT                               = 3
	ROTATE_EVENT                             = 4
	INTVAR_EVENT                             = 5 // Integer based session-variables
	LOAD_EVENT                               = 6
	SLAVE_EVENT                              = 7
	CREATE_FILE_EVENT                        = 8
	APPEND_BLOCK_EVENT                       = 9
	EXEC_LOAD_EVENT                          = 10
	DELETE_FILE_EVENT                        = 11
	NEW_LOAD_EVENT                           = 12 //  NEW_LOAD_EVENT is like LOAD_EVENT except that it has a longer  sql_ex, allowing multibyte TERMINATED BY etc; both types share the same class (Load_event)
	RAND_EVENT                               = 13
	USER_VAR_EVENT                           = 14
	FORMAT_DESCRIPTION_EVENT                 = 15 // MYSQL根据其定义的来解析其他事件
	XID_EVENT                                = 16 // 事务提交(MYSQL进行崩溃恢复时间,根据事务在binlog中的提交情况来决定是否提交存储引擎中状态为prepared的事物)
	BEGIN_LOAD_QUERY_EVENT                   = 17
	EXECUTE_LOAD_QUERY_EVENT                 = 18
	TABLE_MAP_EVENT                          = 19 // 每个ROWS_EVENT事件之前有一个TABLE_MAP_EVENT用于描述内部ID和结构定义
	PRE_GA_WRITE_ROWS_EVENT                  = 20 // The PRE_GA event numbers were used for 5.1.0 to 5.1.15 and are therefore obsolete.
	PRE_GA_UPDATE_ROWS_EVENT                 = 21 // The PRE_GA event numbers were used for 5.1.0 to 5.1.15 and are therefore obsolete.
	PRE_GA_DELETE_ROWS_EVENT                 = 22 // The PRE_GA event numbers were used for 5.1.0 to 5.1.15 and are therefore obsolete.
	WRITE_ROWS_EVENT_V1                      = 23 // The V1 event numbers are used from 5.1.16 until mysql-trunk-xx
	UPDATE_ROWS_EVENT_V1                     = 24 // The V1 event numbers are used from 5.1.16 until mysql-trunk-xx
	DELETE_ROWS_EVENT_V1                     = 25 // The V1 event numbers are used from 5.1.16 until mysql-trunk-xx
	INCIDENT_EVENT                           = 26 //Something out of the ordinary happened on the master
	HEARTBEAT_LOG_EVENT                      = 27 // Heartbeat event to be send by master at its idle time to ensure master's online status to slave
	IGNORABLE_LOG_EVENT                      = 28 // In some situations, it is necessary to send over ignorable data to the slave: data that a slave can handle in case there is code for handling it, but which can be ignored if it is not recognized.
	ROWS_QUERY_LOG_EVENT                     = 29
	WRITE_ROWS_EVENT                         = 30 // Version 2 of the Row events 包含了要插入的数据
	UPDATE_ROWS_EVENT                        = 31 // Version 2 of the Row events 包含了行修改前的值,也包含了修改后的值
	DELETE_ROWS_EVENT                        = 32 // Version 2 of the Row events 包含了需要删除行的主键值/行号
	GTID_LOG_EVENT                           = 33
	ANONYMOUS_GTID_LOG_EVENT                 = 34
	PREVIOUS_GTIDS_LOG_EVENT                 = 35
	TRANSACTION_CONTEXT_EVENT                = 36
	VIEW_CHANGE_EVENT                        = 37
	XA_PREPARE_LOG_EVENT                     = 38 // Prepared XA transaction terminal event similar to Xid
	// Add new events here - right above this comment! Existing events (except ENUM_END_EVENT) should never change their numbers
	ENUM_END_EVENT /* end marker */

	ANNOTATE_ROWS_EVENT     = 0xa0 //160
	BINLOG_CHECKPOINT_EVENT = 0xa1 //161
	GTID_EVENT              = 0xa2 //162
	GTID_LIST_EVENT         = 0xa3 //163
)

const (
	QUERY_HEADER_MINIMAL_LEN            = (4 + 4 + 1 + 2)                // where 3.23, 4.x and 5.0 agree
	QUERY_HEADER_LEN                    = (QUERY_HEADER_MINIMAL_LEN + 2) // where 5.0 differs: 2 for length of N-bytes vars
	STOP_HEADER_LEN                     = 0
	LOAD_HEADER_LEN                     = (4 + 4 + 4 + 1 + 1 + 4)
	START_V3_HEADER_LEN                 = (2 + ST_SERVER_VER_LEN + 4)
	ROTATE_HEADER_LEN                   = 8 // this is FROZEN (the Rotate post-header is frozen
	INTVAR_HEADER_LEN                   = 0
	CREATE_FILE_HEADER_LEN              = 4
	APPEND_BLOCK_HEADER_LEN             = 4
	EXEC_LOAD_HEADER_LEN                = 4
	DELETE_FILE_HEADER_LEN              = 4
	NEW_LOAD_HEADER_LEN                 = LOAD_HEADER_LEN
	RAND_HEADER_LEN                     = 0
	USER_VAR_HEADER_LEN                 = 0
	FORMAT_DESCRIPTION_HEADER_LEN       = (START_V3_HEADER_LEN + 1 + LOG_EVENT_TYPES)
	XID_HEADER_LEN                      = 0
	BEGIN_LOAD_QUERY_HEADER_LEN         = APPEND_BLOCK_HEADER_LEN
	ROWS_HEADER_LEN_V1                  = 8
	TABLE_MAP_HEADER_LEN                = 8
	EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN = (4 + 4 + 4 + 1)
	EXECUTE_LOAD_QUERY_HEADER_LEN       = (QUERY_HEADER_LEN + EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN)
	INCIDENT_HEADER_LEN                 = 2
	HEARTBEAT_HEADER_LEN                = 0
	IGNORABLE_HEADER_LEN                = 0
	ROWS_HEADER_LEN_V2                  = 10
	TRANSACTION_CONTEXT_HEADER_LEN      = 18
	VIEW_CHANGE_HEADER_LEN              = 52
	XA_PREPARE_HEADER_LEN               = 0
)
