package storage

import "testing"

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
	if isNull(bit, null) {
		t.Fatal("Bit is not set: Field is empty")
	}
}