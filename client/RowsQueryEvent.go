package client

import "github.com/SDHM/sqlregret/mysql"

type RowsQueryEvent struct {
	queryString string
}

func ParseRowsQueryEvent(logbuf *mysql.LogBuffer, descriptionEvent *FormatDescriptionLogEvent) *RowsQueryEvent {
	commonHeaderLen := descriptionEvent.GetCommonHeaderLen()
	postHeaderLen := descriptionEvent.PostHeaderLen[mysql.ROWS_QUERY_EVENT-1]
	logbuf.SkipLen(commonHeaderLen + int(postHeaderLen) + 1)

	rowsQueryEvent := new(RowsQueryEvent)
	rowsQueryEvent.queryString = logbuf.GetRestString()
	return rowsQueryEvent
}

func (this *RowsQueryEvent) GetRowsQueryString() string {
	return this.queryString
}
