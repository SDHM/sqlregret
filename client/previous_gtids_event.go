package client

import (
	"fmt"

	. "sqlregret/binlogevent"

	"sqlregret/mysql"
)

type Previous_Gtids_Log_Event struct {
	buf []byte
}

func ParsePreviousGtidLogEvent(logBuf *mysql.LogBuffer, descriptionEvent *FormatDescriptionLogEvent) *Previous_Gtids_Log_Event {
	// common_header_len := descriptionEvent.commonHeaderLen
	post_header_len := descriptionEvent.PostHeaderLen[PREVIOUS_GTIDS_LOG_EVENT-1]

	event := new(Previous_Gtids_Log_Event)
	logBuf.SkipLen(int(post_header_len))
	event.buf = logBuf.GetRestBytes()
	return event
}

func (this *Previous_Gtids_Log_Event) String() string {
	fmt.Printf("% 2x", this.buf)
	return ""
}
