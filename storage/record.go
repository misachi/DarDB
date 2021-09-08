package storage

type Record interface {
	GetField(fieldName string) interface{}
	CreateField(fieldName string, value interface{})
	UpdateField(fieldName string, value interface{}) error
	RemoveField(fieldName string) error
}

type Pair struct {
	First, Second interface{}
}

type RecordHeader struct {
	NullField interface{}
	Location  []Pair
}
type VarLengthRecord struct {
	RecordHeader
	Field []Pair
}
type FixedLengthRecord struct {
	Field []Pair
}
