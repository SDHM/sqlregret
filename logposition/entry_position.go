package logposition

type EntryPosition struct {
	Included    bool
	JournalName string
	Position    int64
	Timesnamp   int64
}
