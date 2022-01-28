package column

import "reflect"

type SUPPORTED_TYPE int

const (
	INT SUPPORTED_TYPE = iota
	INT8
	INT16
	INT32
	INT64
	UINT
	UINT8
	UINT16
	UINT32
	UINT64
	FLOAT32
	FLOAT64
	STRING
)

type Column struct {
	Name  string
	Type SUPPORTED_TYPE
}

func NewColumn(name string, typ SUPPORTED_TYPE) Column {
	return Column{Name: name, Type: typ}
}

func (c Column) size() int {
	return GetTypeSize(c.Type)
}

func GetTypeSize(name SUPPORTED_TYPE) int {
	var val interface{}
	switch name {
	case INT8:
		val = int8(3)
		return int(reflect.TypeOf(val).Size())
	case INT16:
		val = int16(3)
		return int(reflect.TypeOf(val).Size())
	case INT, INT32:
		val = int32(3)
		return int(reflect.TypeOf(val).Size())
	case INT64:
		val = int64(3)
		return int(reflect.TypeOf(val).Size())
	case UINT8:
		val = uint8(3)
		return int(reflect.TypeOf(val).Size())
	case UINT16:
		val = uint16(3)
		return int(reflect.TypeOf(val).Size())
	case UINT, UINT32:
		val = uint32(3)
		return int(reflect.TypeOf(val).Size())
	case UINT64:
		val = uint64(3)
		return int(reflect.TypeOf(val).Size())
	case FLOAT32:
		val = float32(3)
		return int(reflect.TypeOf(val).Size())
	case FLOAT64:
		val = float64(3)
		return int(reflect.TypeOf(val).Size())
	default:
		return -1
	}
}
