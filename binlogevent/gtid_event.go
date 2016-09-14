package binlogevent

const (
	ENCODED_FLAG_LENGTH               = 1
	ENCODED_SID_LENGTH                = 16
	ENCODED_GNO_LENGTH                = 8
	LOGICAL_TIMESTAMP_TYPECODE_LENGTH = 1
	LOGICAL_TIMESTAMP_LENGTH          = 16
	POST_HEADER_LENGTH                = ENCODED_FLAG_LENGTH + ENCODED_SID_LENGTH + ENCODED_GNO_LENGTH + LOGICAL_TIMESTAMP_TYPECODE_LENGTH + LOGICAL_TIMESTAMP_LENGTH /* length of two logical timestamps */
)

type Gtid_event struct {
}
