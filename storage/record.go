package storage

import (
	"fmt"
	"unsafe"
)

type Record interface {
	GetField(fieldName string, idx int) Pair
	CreateField(fieldName string, value interface{})
	UpdateField(fieldName string, value interface{}) error
	RemoveField(fieldName string) error
}

type Pair struct {
	First, Second interface{}
}

func NewPair(first string, second interface{}) (*Pair, error) {
	switch second.(type) {
	case int8:
		return &Pair{first, second.(int8)}, nil
	case int16:
		return &Pair{first, second.(int16)}, nil
	case int32:
		return &Pair{first, second.(int32)}, nil
	case int64:
		return &Pair{first, second.(int64)}, nil
	case float32:
		return &Pair{first, second.(float32)}, nil
	case float64:
		return &Pair{first, second.(float64)}, nil
	case string:
		return &Pair{first, second.(string)}, nil
	default:
		return nil, fmt.Errorf("Unsupported type %T", second)
	}
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

func (v VarLengthRecord) fiedIsNull(idx int) bool { return (v.NullField.(int) & idx) >= 1 }

func (v *VarLengthRecord) GetField(fieldName string, idx int) *Pair {
	if idx >= 0 && !v.fiedIsNull(idx) {
		return &v.Field[idx]
	}
	return nil
}

func (v *VarLengthRecord) CreateField(fieldName string, value interface{}) {
	newField, err := NewPair(fieldName, value)
	if err != nil {
		panic(err)
	}
	location := Pair{len(v.Location), unsafe.Sizeof(newField.Second)}
	v.Location[cap(v.Location)] = location
	v.Field[cap(v.Field)] = *newField
}

func (v *VarLengthRecord) UpdateField(fieldName string, value interface{}) error {

}
