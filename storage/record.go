package storage

import (
	"math/rand"
	"sync"
	"time"

	"github.com/misachi/DarDB/util"
)

type LocationPair struct {
	offset Location_T
	size   Location_T
}

func (l LocationPair) Offset() Location_T { return l.offset }
func (l LocationPair) Size() Location_T   { return l.size }

type recordHeader struct {
	nullField NullField_T
	location  []LocationPair
}

type VarLengthRecord struct {
	recordHeader
	field []byte
	mtx   *sync.Mutex
}

type FixedLengthRecord struct {
	nullField NullField_T
	field     []byte
	mtx       *sync.Mutex
}

// Given the field name return the index in the record
func getFieldIndex(fieldName string) int {
	// TODO Change implementation once catalogue is complete
	rand.Seed(time.Now().UnixNano())
	return rand.Int()
}

func (v *VarLengthRecord) fieldIsNull(bitmask NullField_T) bool {
	return util.IsNull(bitmask, v.nullField)
}
func (v *VarLengthRecord) Location(offset Location_T) *LocationPair {
	for _, loc := range v.location {
		if loc.offset == offset {
			return &loc
		}
	}
	return nil
}

func (v *VarLengthRecord) GetField(offset Location_T, idx NullField_T) []byte {
	location := v.Location(offset)
	if !v.fieldIsNull(idx) && location != nil {
		return v.field[offset : offset+location.size]
	}
	return nil
}

func (v *VarLengthRecord) AddField(fieldName string, value []byte) {
	v.mtx.Lock()
	defer v.mtx.Unlock()

	locLen := len(v.location)
	var bufSize Location_T
	for _, loc := range v.location {
		bufSize += loc.size
	}
	v.location[locLen] = LocationPair{Location_T(len(v.field)), Location_T(len(value))}
	v.field = append(
		v.field[:len(v.field) - int(bufSize)],
		append(value, v.field[len(v.field) - int(bufSize):]...)...)
	fieldIdx := getFieldIndex(fieldName) - locLen
	v.nullField = v.nullField | NullField_T(1)
}

func (v *VarLengthRecord) UpdateField(fieldName string, value []byte) {
	v.mtx.Lock()
	defer v.mtx.Unlock()
	fieldIdx := getFieldIndex(fieldName)
	v.field = append(v.field, value...)
}

func (f FixedLengthRecord) getFieldSize(idx NullField_T) int {
	table := []int{1, 2, 3, 4, 5, 6}
	return table[idx]
}
func (f *FixedLengthRecord) fieldIsNull(bit NullField_T) bool { return util.IsNull(bit, f.nullField) }
func (f *FixedLengthRecord) GetField(offset Location_T, idx NullField_T) []byte {
	if f.fieldIsNull(idx) {
		return nil
	}
	return f.field[offset : offset+Location_T(f.getFieldSize(idx))]
}

func (f *FixedLengthRecord) AddField(fieldName string, value []byte) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.field = append(f.field, value...)
	f.nullField = f.nullField | NullField_T(getFieldIndex(fieldName))
}

func (f *FixedLengthRecord) UpdateField(fieldName string, value []byte) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.field = append(f.field, value...)
}
