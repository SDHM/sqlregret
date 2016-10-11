package client

import (
	"github.com/SDHM/sqlregret/binlogevent"
	"github.com/SDHM/sqlregret/config"
)

// 返回为false 表示不过滤 返回为true 表示过滤
func FilterSkipSQL(eventType int) bool {

	// 如果eventType不是这六个之间的一个, 则不过滤
	if eventType != binlogevent.UPDATE_ROWS_EVENT_V1 &&
		eventType != binlogevent.UPDATE_ROWS_EVENT &&
		eventType != binlogevent.DELETE_ROWS_EVENT_V1 &&
		eventType != binlogevent.DELETE_ROWS_EVENT &&
		eventType != binlogevent.WRITE_ROWS_EVENT_V1 &&
		eventType != binlogevent.WRITE_ROWS_EVENT {
		return false
	}

	switch config.G_filterConfig.FilterSQL {
	case "":
		{
			return false
		}
	case "insert":
		{
			if eventType == binlogevent.UPDATE_ROWS_EVENT_V1 ||
				eventType == binlogevent.UPDATE_ROWS_EVENT ||
				eventType == binlogevent.DELETE_ROWS_EVENT_V1 ||
				eventType == binlogevent.DELETE_ROWS_EVENT {
				return true
			} else if eventType == binlogevent.WRITE_ROWS_EVENT_V1 ||
				eventType == binlogevent.WRITE_ROWS_EVENT {
				return false
			}
		}
	case "delete":
		{
			if eventType == binlogevent.UPDATE_ROWS_EVENT_V1 ||
				eventType == binlogevent.UPDATE_ROWS_EVENT ||
				eventType == binlogevent.WRITE_ROWS_EVENT_V1 ||
				eventType == binlogevent.WRITE_ROWS_EVENT {
				return true
			} else if eventType == binlogevent.DELETE_ROWS_EVENT_V1 ||
				eventType == binlogevent.DELETE_ROWS_EVENT {
				return false
			}
		}
	case "update":
		{
			if eventType == binlogevent.DELETE_ROWS_EVENT_V1 ||
				eventType == binlogevent.DELETE_ROWS_EVENT ||
				eventType == binlogevent.WRITE_ROWS_EVENT_V1 ||
				eventType == binlogevent.WRITE_ROWS_EVENT {
				return true
			} else if eventType == binlogevent.UPDATE_ROWS_EVENT_V1 ||
				eventType == binlogevent.UPDATE_ROWS_EVENT {
				return false
			}
		}
	default:
		{
			return true
		}
	}

	return false
}
