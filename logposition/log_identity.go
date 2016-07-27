package logposition

type LogIdentity struct {
	SourceAddress string
	SlaveId       int64
}

func NewLogIdentity(sourceAddress string, slaveId int64) *LogIdentity {
	logIdentity := new(LogIdentity)
	logIdentity.SourceAddress = sourceAddress
	logIdentity.SlaveId = slaveId
	return logIdentity
}
