package storage

import (
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
		buf []byte
		parsed bool
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
		if isNum := isNumber(val.buf); isNum != val.parsed {
			t.Errorf("Expected parse result to be %v for number(%s) but got %v", val.parsed, val.buf, isNum)
		}
	}
}
