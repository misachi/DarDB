package util

import (
	"reflect"
)

const (
	VarRecord = iota
	FixedRecord
	VarField
	FixedField
)

type Pair struct {
	First, Second interface{}
}

func SizeOf(val reflect.Type) uint {
	switch val.Kind() {
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int, reflect.Int64,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return uint(val.Size())
	case reflect.Struct:
		var sz uint
		for i := 0; i < val.NumField(); i++ {
			s := SizeOf(val.Field(i).Type)
			sz += s
		}
		return sz
	case reflect.Slice:
		if sz := SizeOf(val.Elem()); sz > 0 {
			return sz * uint(val.Len())
		}
		return 0
	case reflect.Array:
		if sz := SizeOf(val.Elem()); sz > 0 {
			return sz * uint(val.Len())
		}
		return 0
	}
	return 0
}
