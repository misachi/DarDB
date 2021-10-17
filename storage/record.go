package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

var ErrColumnDoesNotExist = errors.New("column does not exist")

const Term = '\n'                            // Byte slice terminator
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

func ByteArrayToInt(r io.Reader) (int, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, fmt.Errorf("ByteArrayToInt io.ReadAll error: %v", err)
	}
	val, err := strconv.Atoi(*(*string)(unsafe.Pointer(&data)))
	if err != nil {
		return val, fmt.Errorf("ByteArrayToInt error: %v", err)
	}
	return val, nil
}

func NewVarLengthRecord(data []byte) (*VarLengthRecord, error) {
	termIdx := bytes.IndexByte(data, Term) // first terminator - for nullfield
	newBuf := bytes.NewBuffer(data[:termIdx])
	nField, err := ByteArrayToInt(newBuf)
	if err != nil {
		return nil, fmt.Errorf("NewVarLengthRecord: %v", err)
	}
	locationEnd := bytes.IndexByte(data[termIdx+1:], Term)
	location, err := setLocation(data[termIdx+1 : locationEnd+termIdx+1])
	if err != nil {
		return nil, err
	}
	recHDR := recordHeader{
		nullField: NullField_T(nField),
		location:  *location,
	}
	return &VarLengthRecord{
		recordHeader: recHDR,
		field:        data[locationEnd+termIdx+2:],
		mtx:          &sync.Mutex{},
	}, nil
}

type FixedLengthRecord struct {
	nullField NullField_T
	field     []byte
	mtx       *sync.Mutex
}

const (
	Number = iota + 1
	String
)

type column struct {
	name  string
	_type string
}

func (c column) size() int {
	return getTypeSize(c._type)
}

type columnData struct {
	keys []column
}

func NewColumnData() columnData {
	// returns columns and the associated types
	return columnData{
		keys: []column{
			{name: "field1", _type: "int"},
			{name: "field2", _type: "float32"},
			{name: "field3", _type: "uint42"},
		},
	}
}

func (cd columnData) column(name string) (column, error) {
	for _, key := range cd.keys {
		if name == key.name {
			return key, nil
		}
	}
	return column{}, ErrColumnDoesNotExist
}

func (cd columnData) index(name string) (int, error) {
	for idx, key := range cd.keys {
		if name == key.name {
			return idx, nil
		}
	}
	return -1, ErrColumnDoesNotExist
}

func getTypeSize(name string) int {
	var val interface{}
	switch name {
	case "int8":
		val = int8(3)
		return int(reflect.TypeOf(val).Size())
	case "int16":
		val = int16(3)
		return int(reflect.TypeOf(val).Size())
	case "int", "int32":
		val = int32(3)
		return int(reflect.TypeOf(val).Size())
	case "int64":
		val = int64(3)
		return int(reflect.TypeOf(val).Size())
	case "uint8":
		val = uint8(3)
		return int(reflect.TypeOf(val).Size())
	case "uint16":
		val = uint16(3)
		return int(reflect.TypeOf(val).Size())
	case "uint", "uint32":
		val = uint32(3)
		return int(reflect.TypeOf(val).Size())
	case "uint64":
		val = uint64(3)
		return int(reflect.TypeOf(val).Size())
	case "float32":
		val = float32(3)
		return int(reflect.TypeOf(val).Size())
	case "float64":
		val = float64(3)
		return int(reflect.TypeOf(val).Size())
	default:
		return -1
	}
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

func setLocation(lData []byte) (*[]LocationPair, error) {
	bufSize := len(lData)
	newBuf := make([]byte, bufSize)
	location := make([]LocationPair, 10)
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
		offset, err := ByteArrayToInt(bytes.NewBuffer(newBuf[:fieldSepIdx]))
		if err != nil {
			return nil, fmt.Errorf("setLocation: Unable to set offset: %v", err)
		}

		size, err := ByteArrayToInt(bytes.NewBuffer(newBuf[fieldSepIdx+1 : locSepIdx]))
		if err != nil {
			return nil, fmt.Errorf("setLocation: Unable to set size: %v", err)
		}

		location[idx] = *NewLocationPair(Location_T(offset), Location_T(size))
		idx += 1

		if (locSepIdx + 1) > len(newBuf) {
			break
		}
		newBuf = newBuf[locSepIdx+1:]
	}
	return &location, nil
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
	v.mtx.Lock()
	defer v.mtx.Unlock()

	var newField []byte
	colData := NewColumnData()
	col, err := colData.column(key)

	if err != nil {
		return nil
	}

	idx, _ := colData.index(key)

	if !v.fieldIsNull(NullField_T(idx)) {
		if num := getTypeSize(col._type); num < 0 {
			location := getFieldLocation(idx)
			copy(newField, v.field[location.offset:location.offset+location.size])
			return newField
		} else {
			var fieldIdx int
			for i := 0; i < len(v.field); i++ {
				if fieldIdx < (idx - 1) {
					copy(newField, v.field[i:i+bytes.Index(
						v.field[i:], []byte{Term})])
					return newField
				}
				if v.field[i] == Term {
					fieldIdx += 1
				}
			}
		}

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
	f.mtx.Lock()
	defer f.mtx.Unlock()
	var newField []byte
	idx := NullField_T(getFieldIndex(key))
	if f.fieldIsNull(idx) {
		return nil
	}
	location := getFieldLocation(int(idx))
	copy(newField, f.field[location.offset:location.offset+Location_T(f.getFieldSize(idx))])
	return newField
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
