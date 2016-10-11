package client

import (
	"time"

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

func FilterMode(eventType int) bool {
	if config.G_filterConfig.Mode == "mark" {
		if eventType == binlogevent.FORMAT_DESCRIPTION_EVENT ||
			eventType == binlogevent.TABLE_MAP_EVENT {
			return false
		} else {
			return true
		}
	} else if config.G_filterConfig.Mode == "parse" {
		return false
	} else {
		return false
	}
}

func FilterTime(timeSnap time.Time, eventType int) bool {
	if config.G_filterConfig.StartTimeEnable() && config.G_filterConfig.EndTimeEnable() {
		//开始时间和结束时间都设置了
		if timeSnap.After(config.G_filterConfig.StartTime) && timeSnap.Before(config.G_filterConfig.EndTime) {
			return false
		} else {
			//时间在两者之外，并且不是修改操作的直接跳过
			if eventType == binlogevent.WRITE_ROWS_EVENT_V1 || eventType == binlogevent.WRITE_ROWS_EVENT ||
				eventType == binlogevent.UPDATE_ROWS_EVENT_V1 || eventType == binlogevent.UPDATE_ROWS_EVENT ||
				eventType == binlogevent.DELETE_ROWS_EVENT_V1 || eventType == binlogevent.DELETE_ROWS_EVENT {
				return true
			} else {
				return false
			}
		}
	} else {
		return false
	}
}
