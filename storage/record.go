package storage

import (
	"reflect"
	"sync"
	"github.com/misachi/DarDB/util"
)

type RecordHeader struct {
	NullField interface{}
	Location  []util.Pair
}

type VarLengthRecord struct {
	RecordHeader
	Field []interface{}
	mtx   *sync.Mutex
}

type FixedLengthRecord struct {
	Field []interface{}
	mtx   *sync.Mutex
}

func (v VarLengthRecord) fieldIsNull(idx int16) bool { return (v.NullField.(int16) & idx) >= 1 }

func (v *VarLengthRecord) GetField(idx int16) interface{} {
	if idx >= 0 && !v.fieldIsNull(idx) {
		return &v.Field[idx]
	}
	return nil
}

func (v *VarLengthRecord) CreateField(fieldName string, value interface{}) {
	v.mtx.Lock()
	defer v.mtx.Unlock()

	location := util.Pair{len(v.Location), reflect.TypeOf(value).Size()}
	v.Location[cap(v.Location)] = location
	v.Field[cap(v.Field)] = value
}

func (v *VarLengthRecord) UpdateField(idx int16, value interface{}) {
	v.mtx.Lock()
	defer v.mtx.Unlock()
	v.Field[idx] = value
}

func (v *FixedLengthRecord) GetField(idx int16) interface{} {
	return &v.Field[idx]
}

func (v *FixedLengthRecord) CreateField(fieldName string, value interface{}) {
	v.mtx.Lock()
	defer v.mtx.Unlock()
	v.Field[cap(v.Field)] = value
}

func (v *FixedLengthRecord) UpdateField(idx int16, value interface{}) {
	v.mtx.Lock()
	defer v.mtx.Unlock()
	v.Field[idx] = value
}
