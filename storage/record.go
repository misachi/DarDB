package storage

import (
	"math/rand"
	"sync"
	"time"
)

func IsNull(bit, nullField NullField_T) bool { return (nullField & bit) < 1 }

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

func NewVarLengthRecord() {
	cols, size := getColType()
}

type FixedLengthRecord struct {
	nullField NullField_T
	field     []byte
	mtx       *sync.Mutex
}

// getColType returns columns and the associated sizes
func getColType() ([][]byte, []int) {
	return [][]byte{
		[]byte("field1"),
		[]byte("field2"),
		[]byte("field3"),
	}, []int{2, 4, 8}
}

// Given the field name return the index in the record
func getFieldIndex(key string) int {
	// TODO Change implementation once catalogue is complete
	rand.Seed(time.Now().UnixNano())
	return rand.Int()
}

func getFieldLocation(idx int) LocationPair {
	// TODO Change implementation once catalogue is complete
	return LocationPair{}
}

func (v *VarLengthRecord) fieldIsNull(bitmask NullField_T) bool {
	return IsNull(bitmask, v.nullField)
}

func (v *VarLengthRecord) Location(offset Location_T) *LocationPair {
	for _, loc := range v.location {
		if loc.offset == offset {
			return &loc
		}
	}
	return nil
}

func (v *VarLengthRecord) GetField(key string) []byte {
	idx := getFieldIndex(key)
	if !v.fieldIsNull(NullField_T(idx)) {
		location := getFieldLocation(idx)
		return v.field[location.offset : location.offset+location.size]
	}
	return nil
}

func isNumber(value []byte) bool {
	if len(value) > 1 && value[0] == '-' {
		value = value[1:]
	}

	for i := 0; i < len(value); i++ {
		switch {
		case value[i] >= '0' && value[i] <= '9':
			continue
		case value[i] == '.' && len(value[1:]) >= 1:
			return isNumber(value[1:])
		case (value[i] == 'e' || value[i] == 'E') && len(value[1:]) >= 1:
			return isNumber(value[1:])
		default:
			return false
		}
	}
	return true
}

func (v *VarLengthRecord) AddField(key string, value []byte) {
	v.mtx.Lock()
	defer v.mtx.Unlock()

	if !isNumber(value) {
		locLen := len(v.location)
		v.location[locLen] = LocationPair{Location_T(len(v.field)), Location_T(len(value))}
	}
	var bufSize Location_T
	for _, loc := range v.location {
		bufSize += loc.size
	}
	v.field = append(
		v.field[:len(v.field)-int(bufSize)],
		append(value, v.field[len(v.field)-int(bufSize):]...)...,
	)
	fieldIdx := getFieldIndex(key)
	v.nullField = v.nullField | (1 << NullField_T(fieldIdx))
}

func (v *VarLengthRecord) UpdateField(key string, value []byte) {
	v.mtx.Lock()
	defer v.mtx.Unlock()
	location := getFieldLocation(getFieldIndex(key))
	v.field = append(
		v.field[:location.offset],
		append(value, v.field[location.offset+location.size:]...)...)
}

func (f FixedLengthRecord) getFieldSize(idx NullField_T) int {
	table := []int{1, 2, 3, 4, 5, 6}
	return table[idx]
}
func (f *FixedLengthRecord) fieldIsNull(bit NullField_T) bool { return IsNull(bit, f.nullField) }
func (f *FixedLengthRecord) GetField(key string) []byte {
	idx := NullField_T(getFieldIndex(key))
	if f.fieldIsNull(idx) {
		return nil
	}
	location := getFieldLocation(int(idx))
	return f.field[location.offset : location.offset+Location_T(f.getFieldSize(idx))]
}

func (f *FixedLengthRecord) AddField(key string, value []byte) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.field = append(f.field, value...)
	f.nullField = f.nullField | (1 << NullField_T(getFieldIndex(key)))
}

func (f *FixedLengthRecord) UpdateField(key string, value []byte) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	location := getFieldLocation(getFieldIndex(key))
	f.field = append(
		f.field[:location.offset],
		append(value, f.field[location.offset+location.size:]...)...)
}
