package row

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"unsafe"

	"github.com/misachi/DarDB/column"
	st "github.com/misachi/DarDB/storage"
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

type Record interface {
	GetField(colData ColumnData, key string) []byte
	UpdateField(colData ColumnData, key string, value []byte)
	AddField(colData ColumnData, key string, value []byte)
	LockRecord(lType uint8)
	UnLockRecord()
}

func IsNull(bit, nullField st.NullField_T) bool { return (nullField & (1 << bit)) < 1 }

type LocationPair struct {
	offset st.Location_T
	size   st.Location_T
}

func NewLocationPair(offset, size st.Location_T) *LocationPair {
	return &LocationPair{
		offset: offset,
		size:   size,
	}
}

func (l LocationPair) Offset() st.Location_T { return l.offset }
func (l LocationPair) Size() st.Location_T   { return l.size }
func (l *LocationPair) SetOffset(offset st.Location_T) { l.offset = offset }
func (l *LocationPair) SetSize(size st.Location_T) { l.size = size }

type recordHeader struct {
	isLocked  bool
	nullField st.NullField_T
	rowLock   *st.Lock
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
	mu := st.NewLock()
	if len(data) < 1 {
		return &VarLengthRecord{
			recordHeader: recordHeader{false, 0, mu, []LocationPair{{0, 0}}},
			field:        []byte{},
		}, nil
	}
	var nullField st.NullField_T
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

		var offset st.Location_T = 0

		if key.Type == column.STRING {
			if i > 0 && cols[i-1].Type != column.STRING {
				field = append(field, '\n')
			}
			offset = location[len(location)-1].offset + location[len(location)-1].size + 1
		} else {
			if i > 0 {
				field = append(field, ':')
				offset = location[len(location)-1].offset + location[len(location)-1].size + 1
			}
		}
		location = append(location, *NewLocationPair(offset, st.Location_T(_len)))
		field = append(field, data[i]...)
	}
	return &VarLengthRecord{
		recordHeader: recordHeader{isLocked: false, rowLock: mu, nullField: nullField, location: location},
		field:        field,
	}, nil
}

func NewVarLengthRecordWithHDR(data []byte) (*VarLengthRecord, error) {
	mu := st.NewLock()
	copyData := make([]byte, len(data))
	copy(copyData, data)

	if len(data) < 1 {
		return &VarLengthRecord{
			recordHeader: recordHeader{false, 0, mu, []LocationPair{{0, 0}}},
			field:        []byte{},
		}, nil
	}

	termIdx := bytes.IndexByte(copyData, Term) // first terminator - for nullfield
	newBuf := bytes.NewReader(copyData[:termIdx])
	nField, err := ByteArrayToInt(newBuf)
	if err != nil {
		return nil, fmt.Errorf("NewVarLengthRecordWithHDR: %v", err)
	}

	locationEnd := bytes.IndexByte(copyData[termIdx+1:], Term)
	location, err := setLocation(copyData[termIdx+1 : locationEnd+termIdx+1])
	if err != nil {
		return nil, fmt.Errorf("NewVarLengthRecordWithHDR: %v", err)
	}
	recHDR := recordHeader{
		isLocked:  false,
		rowLock:   mu,
		nullField: st.NullField_T(nField),
		location:  *location,
	}
	return &VarLengthRecord{
		recordHeader: recHDR,
		field:        copyData[locationEnd+termIdx+2:],
	}, nil
}

type FixedLengthRecord struct {
	// isLocked  bool
	nullField st.NullField_T
	field     []byte
	rowLock   *st.Lock
	mtx       *sync.Mutex
}

type ColumnData struct {
	keys []column.Column
}

func NewColumnData_(columns []column.Column) ColumnData {
	return ColumnData{columns}
}

func (cd ColumnData) column(name string) (column.Column, error) {
	for _, key := range cd.keys {
		if name == key.Name {
			return key, nil
		}
	}
	return column.Column{}, ErrColumnDoesNotExist
}

func (cd ColumnData) index(name string) (int, error) {
	for idx, key := range cd.keys {
		if name == key.Name {
			return idx, nil
		}
	}
	return -1, ErrColumnDoesNotExist
}

func getFieldLocation(cols ColumnData, location []LocationPair, key string) *LocationPair {
	for i, cKey := range cols.keys {
		if key == cKey.Name {
			return &location[i]
		}
	}
	return nil
}

func setLocation(lData []byte) (*[]LocationPair, error) {
	bufSize := len(lData)
	newBuf := make([]byte, bufSize)
	var location []LocationPair

	if numCopy := copy(newBuf, lData); numCopy != bufSize {
		return nil, fmt.Errorf("setLocation copy error: expected to copy %d elements but got %d", bufSize, numCopy)
	}
	locSep := ':'   // Location separator
	fieldSep := ',' // Separator between offset and size
	idx := 0

	for len(newBuf) > 0 {
		locSepIdx := bytes.IndexByte(newBuf, byte(locSep))
		fieldSepIdx := bytes.IndexByte(newBuf, byte(fieldSep))

		if fieldSepIdx == -1 && locSepIdx == -1 {
			break
		}

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

		if size > 0 {
			if len(location) <= 0 {
				location = []LocationPair{*NewLocationPair(st.Location_T(offset), st.Location_T(size))}
			} else {
				location = append(location, *NewLocationPair(st.Location_T(offset), st.Location_T(size)))
			}
		}

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

func (v VarLengthRecord) fieldIsNull(bitmask st.NullField_T) bool {
	return IsNull(bitmask, v.nullField)
}

func (v *VarLengthRecord) LockRecord(lType uint8) {
	v.rowLock.AcquireLock(lType)
}

func (v *VarLengthRecord) UnLockRecord() {
	v.rowLock.ReleaseLock()
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

func (v VarLengthRecord) Location(offset st.Location_T) *LocationPair {
	for _, loc := range v.location {
		if loc.offset == offset {
			return &loc
		}
	}
	return nil
}

func (v *VarLengthRecord) updateLocation(locIdx int, location LocationPair, offset, size st.Location_T) {
	if size != location.size {
		v.location[locIdx].size = size

		if locIdx >= 1 {
			for ; locIdx < len(v.location); locIdx++ {

				v.location[locIdx].offset = v.location[locIdx-1].offset + v.location[locIdx-1].size
				prevLoc := v.location[locIdx-1].offset + v.location[locIdx-1].size
				if locIdx == 2 {
					fmt.Println(prevLoc+1)
					fmt.Printf("Fields: %q\n", v.field)
					fmt.Printf("Byte: %q\n", v.field[10])
					fmt.Printf("%v\n", (v.field[prevLoc+1] == Term || v.field[prevLoc+1] == FieldSep) || (v.location[locIdx-1].offset == 0))
				}

				if (v.field[prevLoc] == Term || v.field[prevLoc] == FieldSep) || (v.location[locIdx-1].offset == 0) {
					v.location[locIdx].offset = v.location[locIdx-1].offset + v.location[locIdx-1].size + 1
				}

			}
		}
	}
}

func (v VarLengthRecord) GetField(colData ColumnData, key string) []byte {
	col, err := colData.column(key)
	if err != nil {
		return nil
	}

	idx, _ := colData.index(key)
	if !v.fieldIsNull(st.NullField_T(idx)) {
		if num := column.GetTypeSize(col.Type); num < 0 {
			location := getFieldLocation(colData, v.location, key)

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

func (v *VarLengthRecord) AddField(colData ColumnData, key string, value []byte) {
	var bufSize st.Location_T
	for _, loc := range v.location {
		bufSize += loc.size
	}

	isNum := isNumber(value)
	if !isNum {
		locLen := len(v.field) - int(bufSize)
		v.location[locLen] = LocationPair{st.Location_T(locLen), st.Location_T(len(value))}
	}

	if isNum && !bytes.Contains(value, []byte{Term}) {
		value = append(value, Term)
	}
	v.field = append(
		v.field[:len(v.field)-int(bufSize)],
		append(value, v.field[len(v.field)-int(bufSize):]...)...,
	)

	fieldIdx, _ := colData.index(key)
	v.nullField = v.nullField | (1 << st.NullField_T(fieldIdx))
}

func (v *VarLengthRecord) UpdateField(colData ColumnData, key string, value []byte) {
	idx, _ := colData.index(key)
	offset := 0

	location := getFieldLocation(colData, v.location, key)
	if !isNumber(value) {
		v.field = append(v.field[:location.offset],
			append(value, v.field[location.offset+location.size:]...)...)
		v.updateLocation(idx, *location, location.offset, st.Location_T(len(value)))
		return
	}

	if len(value) <= 0 && !v.fieldIsNull(st.NullField_T(idx)) {
		// Toggle field if value is empty
		v.nullField ^= (1 << st.NullField_T(idx))
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
	v.updateLocation(idx, *location, location.offset, st.Location_T(len(value)))
}

// TODO: Update FixedLength Record methods
func (f FixedLengthRecord) getFieldSize(idx st.NullField_T) int {
	table := []int{1, 2, 3, 4, 5, 6}
	return table[idx]
}
func (f *FixedLengthRecord) fieldIsNull(bit st.NullField_T) bool { return IsNull(bit, f.nullField) }
func (f *FixedLengthRecord) GetField(key string) []byte {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	var newField []byte
	idx := st.NullField_T(f.getFieldSize(0))
	if f.fieldIsNull(idx) {
		return nil
	}
	// location := getFieldLocation(f.location, int(idx))
	location := LocationPair{}
	copy(newField, f.field[location.offset:location.offset+st.Location_T(f.getFieldSize(idx))])
	return newField
}

func (f *FixedLengthRecord) AddField(key string, value []byte) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.field = append(f.field, value...)
	// f.nullField = f.nullField | (1 << st.NullField_T(getTypeSize(key)))
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

func (f *FixedLengthRecord) LockRecord(lType uint8) {
	f.rowLock.AcquireLock(lType)
}

func (f *FixedLengthRecord) UnLockRecord() {
	f.rowLock.ReleaseLock()
}
