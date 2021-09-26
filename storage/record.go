package storage

import (
	"sync"
)

type Location_T uint16 // Type Location offset and size
type NullField_T int

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

func isNull(bit, nullField NullField_T) bool                    { return (nullField & bit) < 1 }
func (v *VarLengthRecord) fieldIsNull(bitmask NullField_T) bool { return isNull(bitmask, v.nullField) }
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

	location := LocationPair{Location_T(len(v.location)), Location_T(len(value))}
	v.location[Location_T(len(v.location))] = location
	v.field = append(v.field, value...)
}

func (v *VarLengthRecord) UpdateField(idx int16, value []byte) {
	v.mtx.Lock()
	defer v.mtx.Unlock()
	v.field = append(v.field, value...)
}

func (f FixedLengthRecord) getFieldSize(idx NullField_T) int {
	table := []int{1, 2, 3, 4, 5, 6}
	return table[idx]
}
func (f *FixedLengthRecord) fieldIsNull(bit NullField_T) bool { return isNull(bit, f.nullField) }
func (f *FixedLengthRecord) GetField(offset Location_T, idx NullField_T) []byte {
	return f.field[:]
	return f.field[offset : offset+Location_T(f.getFieldSize(idx))]
}

func (f *FixedLengthRecord) AddField(fieldName string, value []byte) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.field = append(f.field, value...)
}

func (f *FixedLengthRecord) UpdateField(idx int16, value []byte) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.field = append(f.field, value...)
}
