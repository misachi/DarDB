package storage

import (
	"bytes"
	"testing"
)

func TestLocationPairOffset(t *testing.T) {
	off, size := Location_T(1), Location_T(2)
	lp := LocationPair{off, size}

	if lp.Offset() != off {
		t.Fatalf("Offset %d should be equal to %d", off, lp.Offset())
	}
}

func TestLocationPairSize(t *testing.T) {
	off, size := Location_T(1), Location_T(2)
	lp := LocationPair{off, size}

	if lp.Size() != size {
		t.Fatalf("Size %d should be equal to %d", size, lp.Offset())
	}
}

func TestIsNull(t *testing.T) {
	bit := NullField_T(1) << 1
	null := NullField_T(2)
	if IsNull(bit, null) {
		t.Fatal("Bit is not set: Field is empty")
	}
}

func TestIsNumber(t *testing.T) {
	type valType struct {
		givenBuf   []byte
		wantParsed bool
	}
	values := []valType{
		{[]byte("12345"), true},
		{[]byte("-3798"), true},
		{[]byte("1.23u4"), false},
		{[]byte(".123"), true},
		{[]byte("."), false},
		{[]byte(".e"), false},
		{[]byte("abcdef"), false},
		{[]byte("12.1e-12"), true},
		{[]byte("-123.456"), true},
		{[]byte("123."), false},
		{[]byte("-"), false},
	}

	for _, val := range values {
		// var isNum bool
		if isNum := isNumber(val.givenBuf); isNum != val.wantParsed {
			t.Errorf("Expected parse result to be %v for number(%s) but got %v", val.wantParsed, val.givenBuf, isNum)
		}
	}
}

func TestGetTypeSize(t *testing.T) {
	type valType struct {
		given    string
		wantSize int
	}
	values := []valType{
		{"int8", 1},
		{"int16", 2},
		{"int", 4},
		{"int32", 4},
		{"int64", 8},
		{"uint8", 1},
		{"uint16", 2},
		{"uint", 4},
		{"uint32", 4},
		{"uint64", 8},
		{"float32", 4},
		{"float64", 8},
	}

	for _, val := range values {
		if size := getTypeSize(val.given); size != val.wantSize {
			t.Errorf("Expected size: %d for type %s but got size: %d", val.wantSize, val.given, size)
		}
	}
}

func TestNewVarLengthRecord(t *testing.T) {
	data := []byte("4\n0,2:2,3:5,2\nitwasyou")
	record, err := NewVarLengthRecord(data)
	if err != nil {
		t.Errorf("Create record error: %v", err)
	}

	if record.nullField != 4 {
		t.Errorf("Expected nullField to be %d but got %d", 4, record.nullField)
	}
	if !bytes.Equal(record.field, []byte("itwasyou")) {
		t.Errorf("Field should be equal: %s", record.field)
	}
}

func TestSetLocation(t *testing.T) {
	type valType struct {
		given     []byte
		wantValue []LocationPair
	}
	values := []valType{
		{
			[]byte("0,2:2,3:5,2"), []LocationPair{{0, 2}, {2, 3}, {5, 2}},
		},
		{
			[]byte("0,10:10,50:60,100:160,20"), []LocationPair{{0, 10}, {10, 50}, {60, 100}, {160, 20}},
		},
		{
			[]byte("2,3"), []LocationPair{{2, 3}},
		},
	}
	for _, val := range values {
		loc, err := setLocation(val.given)
		if err != nil {
			t.Errorf("TestSetLocation: %v", err)
		}

		for i := 0; i < len(val.wantValue); i++ {
			lp := *loc
			if lp[i].offset != val.wantValue[i].offset || lp[i].size != val.wantValue[i].size {
				t.Errorf("Expected offset: %d and size: %d\nbut got\noffset: %d and size: %d", val.wantValue[i].offset, val.wantValue[i].size, lp[i].offset, lp[i].size)
			}
		}
	}
}
