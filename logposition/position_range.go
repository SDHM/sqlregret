package logposition

type PositionRange struct {
	Start *LogPosition
	Ack   *LogPosition
	End   *LogPosition
}

func NewPositonRange(s, e *LogPosition) *PositionRange {

	positionRange := new(PositionRange)
	positionRange.Start = s
	positionRange.End = e

	return positionRange
}
