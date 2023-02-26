package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"unsafe"

	"github.com/misachi/DarDB/column"
)

var ErrColumnDoesNotExist = errors.New("column does not exist")

const (
	Term        = '\n' // Byte slice terminator
	FieldSep    = ':'  //
	LocationSep = ','
)

const (
	Number = iota + 1
	String
)

func IsNull(bit, nullField NullField_T) bool { return (nullField & (1 << bit)) < 1 }

type LocationPair struct {
	offset Location_T
	size   Location_T
}

func NewLocationPair(offset, size Location_T) *LocationPair {
	return &LocationPair{
		offset: offset,
		size:   size,
	}
}

func (l LocationPair) Offset() Location_T { return l.offset }
func (l LocationPair) Size() Location_T   { return l.size }

type recordHeader struct {
	isLocked  bool
	rowLock  *Lock
	nullField NullField_T
	location  []LocationPair
}

type VarLengthRecord struct {
	recordHeader
	field []byte
}

func ByteArrayToInt(r io.Reader) (int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, fmt.Errorf("ByteArrayToInt io.ReadAll error: %v", err)
	}
	val, err := strconv.ParseInt(*(*string)(unsafe.Pointer(&data)), 10, 32)
	if err != nil {
		return val, fmt.Errorf("ByteArrayToInt strconv.Atoi error: %v", err)
	}
	return val, nil
}

func NewVarLengthRecord(cols []column.Column, data [][]byte) (*VarLengthRecord, error) {
	mu := NewLock()
	if len(data) < 1 {
		return &VarLengthRecord{
			recordHeader: recordHeader{false, mu, 0, []LocationPair{{0, 0}}},
			field:        []byte{},
		}, nil
	}
	var nullField NullField_T
	var location []LocationPair
	field := make([]byte, 0)
	dataLen := len(data)
	for i, key := range cols {
		if i >= dataLen {
			nullField = nullField & ^(1 << i)
			continue
		}
		_len := len(data[i])
		if _len > 0 {
			nullField = nullField | (1 << i)
		}
		if column.GetTypeSize(key.Type) < 0 {
			var offset Location_T
			if len(location) < 1 {
				field = append(field, '\n')
				offset = 0
			} else {
				offset = location[len(location)-1].offset + location[len(location)-1].size + 1
			}
			location = append(location, *NewLocationPair(offset, Location_T(_len)))
		} else {
			if i > 0 {
				field = append(field, ':')
			}
		}
		field = append(field, data[i]...)
	}
	return &VarLengthRecord{
		recordHeader: recordHeader{isLocked: false, rowLock: mu, nullField: nullField, location: location},
		field:        field,
	}, nil
}

func NewVarLengthRecordWithHDR(data []byte) (*VarLengthRecord, error) {
	mu := NewLock()
	if len(data) < 1 {
		return &VarLengthRecord{
			recordHeader: recordHeader{false, mu, 0, []LocationPair{{0, 0}}},
			field:        []byte{},
		}, nil
	}
	termIdx := bytes.IndexByte(data, Term) // first terminator - for nullfield
	newBuf := bytes.NewReader(data[:termIdx])
	nField, err := ByteArrayToInt(newBuf)
	if err != nil {
		return nil, fmt.Errorf("NewVarLengthRecordWithHDR: %v", err)
	}
	locationEnd := bytes.IndexByte(data[termIdx+1:], Term)
	location, err := setLocation(data[termIdx+1 : locationEnd+termIdx+1])
	if err != nil {
		return nil, fmt.Errorf("NewVarLengthRecordWithHDR: %v", err)
	}
	recHDR := recordHeader{
		isLocked: false,
		rowLock: mu,
		nullField: NullField_T(nField),
		location:  *location,
	}
	return &VarLengthRecord{
		recordHeader: recHDR,
		field:        data[locationEnd+termIdx+2:],
	}, nil
}

type FixedLengthRecord struct {
	nullField NullField_T
	field     []byte
	mtx       *sync.Mutex
}

type columnData struct {
	keys []column.Column
}

func (cd columnData) column(name string) (column.Column, error) {
	for _, key := range cd.keys {
		if name == key.Name {
			return key, nil
		}
	}
	return column.Column{}, ErrColumnDoesNotExist
}

func (cd columnData) index(name string) (int, error) {
	for idx, key := range cd.keys {
		if name == key.Name {
			return idx, nil
		}
	}
	return -1, ErrColumnDoesNotExist
}

func getFieldLocation(cols columnData, location []LocationPair, idx int) *LocationPair {
	// cols := NewColumnData()
	for i, key := range cols.keys {
		if column.GetTypeSize(key.Type) < 0 {
			return &location[idx-i]
		}
	}
	return nil
}

func setLocation(lData []byte) (*[]LocationPair, error) {
	bufSize := len(lData)
	newBuf := make([]byte, bufSize)
	location := make([]LocationPair, 0)
	if numCopy := copy(newBuf, lData); numCopy != bufSize {
		return nil, fmt.Errorf("setLocation copy error: expected to copy %d elements but got %d", bufSize, numCopy)
	}
	locSep := ':'   // Location separator
	fieldSep := ',' // Separator between offset and size
	idx := 0

	for len(newBuf) > 0 {
		locSepIdx := bytes.IndexByte(newBuf, byte(locSep))
		fieldSepIdx := bytes.IndexByte(newBuf, byte(fieldSep))
		if locSepIdx == -1 {
			locSepIdx = len(newBuf)
		}
		offset, err := ByteArrayToInt(bytes.NewReader(newBuf[:fieldSepIdx]))
		if err != nil {
			return nil, fmt.Errorf("setLocation: Unable to set offset: %v", err)
		}

		size, err := ByteArrayToInt(bytes.NewReader(newBuf[fieldSepIdx+1 : locSepIdx]))
		if err != nil {
			return nil, fmt.Errorf("setLocation: Unable to set size: %v", err)
		}

		location = append(location, *NewLocationPair(Location_T(offset), Location_T(size)))
		idx += 1

		if (locSepIdx + 1) > len(newBuf) {
			break
		}
		newBuf = newBuf[locSepIdx+1:]
	}
	return &location, nil
}

func (v VarLengthRecord) Field() []byte {
	return v.field
}

func (v VarLengthRecord) fieldIsNull(bitmask NullField_T) bool {
	return IsNull(bitmask, v.nullField)
}

func intToByte(i int) []byte {
	return []byte(strconv.Itoa(int(i)))
}

func (v VarLengthRecord) ToByte() []byte {
	retData := intToByte(int(v.nullField))
	retData = append(retData, Term)
	locSize := len(v.location)
	for i, location := range v.location {
		retData = append(retData, intToByte(int(location.offset))...)
		retData = append(retData, LocationSep)
		retData = append(retData, intToByte(int(location.size))...)
		if i < (locSize - 1) {
			retData = append(retData, FieldSep)
		}
	}
	retData = append(retData, Term)
	retData = append(retData, v.field...)
	return retData
}

func (v VarLengthRecord) RecordSize() int {
	return len(v.ToByte())
}

func (v VarLengthRecord) Location(offset Location_T) *LocationPair {
	for _, loc := range v.location {
		if loc.offset == offset {
			return &loc
		}
	}
	return nil
}

func (v *VarLengthRecord) updateLocation(location LocationPair, offset, size Location_T) {
	for i, loc := range v.location {
		if loc.offset == location.offset {
			v.location[i].size = size

			// Next location offset and size have changed
			for j := 1; j < len(v.location[i:]); j++ {
				v.location[i+j].offset = v.location[i+j-1].offset + v.location[i+j-1].size
			}
			break
		}
	}

}

func (v VarLengthRecord) GetField(colData columnData, key string) []byte {
	// colData := NewColumnData()
	col, err := colData.column(key)

	if err != nil {
		return nil
	}

	idx, _ := colData.index(key)
	if !v.fieldIsNull(NullField_T(idx)) {
		if num := column.GetTypeSize(col.Type); num < 0 {
			location := getFieldLocation(colData, v.location, idx)
			if location == nil {
				return nil
			}
			newField := make([]byte, location.size)
			copy(newField, v.field[location.offset:location.offset+location.size])
			return newField
		}
		byteLen := bytes.IndexByte(v.field, Term)
		if byteLen == -1 {
			return nil
		}
		newField := make([]byte, byteLen)
		copy(newField, v.field[:byteLen])
		offset := 0
		for i := 0; i < idx; i++ {
			_idx := bytes.IndexByte(newField[offset:], FieldSep)
			if _idx == -1 {
				break
			}
			offset += len(newField[:_idx+1])
		}
		if _idx := bytes.IndexByte(newField[offset:], FieldSep); _idx > -1 {
			return newField[offset : _idx+offset]
		}
		return newField[offset:]
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

func (v *VarLengthRecord) AddField(colData columnData, key string, value []byte) {

	var bufSize Location_T
	for _, loc := range v.location {
		bufSize += loc.size
	}
	isNum := isNumber(value)
	if !isNum {
		locLen := len(v.field) - int(bufSize)
		v.location[locLen] = LocationPair{Location_T(locLen), Location_T(len(value))}
	}
	if isNum && !bytes.Contains(value, []byte{Term}) {
		value = append(value, Term)
	}
	v.field = append(
		v.field[:len(v.field)-int(bufSize)],
		append(value, v.field[len(v.field)-int(bufSize):]...)...,
	)
	// fieldIdx := getFieldIndex(key)
	// colData := NewColumnData()
	// col, err := colData.column(key)

	// if err != nil {
	// 	return nil
	// }

	fieldIdx, _ := colData.index(key)
	v.nullField = v.nullField | (1 << NullField_T(fieldIdx))
}

func (v *VarLengthRecord) UpdateField(colData columnData, key string, value []byte) {
	// colData := NewColumnData()
	idx, _ := colData.index(key)
	offset := 0
	if !isNumber(value) {
		location := getFieldLocation(colData, v.location, idx)
		v.field = append(v.field[:location.offset],
			append(value, v.field[location.offset+location.size:]...)...)
		// location.size = Location_T(len(value))
		v.updateLocation(*location, location.offset, Location_T(len(value)))
		return
	}
	if len(value) <= 0 && !v.fieldIsNull(NullField_T(idx)) {
		// Toggle field if value is empty
		v.nullField ^= (1 << NullField_T(idx))
	}
	for i := 0; i < idx; i++ {
		_idx := bytes.IndexByte(v.field[offset:], FieldSep)
		if _idx == -1 {
			break
		}
		offset += len(v.field[:_idx+1])
	}
	_idx := bytes.IndexByte(v.field[offset:], FieldSep)
	if _idx < 0 {
		i := bytes.IndexByte(v.field[offset:], Term)
		v.field = append(v.field[:offset], append(value, v.field[offset+i:]...)...)
	} else {
		v.field = append(v.field[:offset], append(value, v.field[offset+_idx:]...)...)
	}
}

// TODO: Update FixedLength Record methods
func (f FixedLengthRecord) getFieldSize(idx NullField_T) int {
	table := []int{1, 2, 3, 4, 5, 6}
	return table[idx]
}
func (f *FixedLengthRecord) fieldIsNull(bit NullField_T) bool { return IsNull(bit, f.nullField) }
func (f *FixedLengthRecord) GetField(key string) []byte {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	var newField []byte
	idx := NullField_T(f.getFieldSize(0))
	if f.fieldIsNull(idx) {
		return nil
	}
	// location := getFieldLocation(f.location, int(idx))
	location := LocationPair{}
	copy(newField, f.field[location.offset:location.offset+Location_T(f.getFieldSize(idx))])
	return newField
}

func (f *FixedLengthRecord) AddField(key string, value []byte) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.field = append(f.field, value...)
	// f.nullField = f.nullField | (1 << NullField_T(getTypeSize(key)))
}

func (f *FixedLengthRecord) UpdateField(key string, value []byte) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	// location := getFieldLocation(getFieldIndex(key))
	location := LocationPair{}
	f.field = append(
		f.field[:location.offset],
		append(value, f.field[location.offset+location.size:]...)...)
}
