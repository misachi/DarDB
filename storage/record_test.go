package storage

import (
	"bytes"
	"testing"

	"github.com/misachi/DarDB/column"
)

func NewColumnData() columnData {
	// returns columns and the associated types
	return columnData{
		keys: []column.Column{
			{Name: "field1", Type: column.INT},
			{Name: "field2", Type: column.FLOAT32},
			{Name: "field3", Type: column.UINT32},
			{Name: "field4", Type: column.INT64},
			{Name: "field5", Type: column.STRING},
			{Name: "field6", Type: column.STRING},
			{Name: "field7", Type: column.STRING},
			{Name: "field8", Type: column.STRING},
		},
	}
}

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
	bit := NullField_T(1)
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
		given    column.SUPPORTED_TYPE
		wantSize int
	}
	values := []valType{
		{column.INT8, 1},
		{column.INT16, 2},
		{column.INT, 4},
		{column.INT32, 4},
		{column.INT64, 8},
		{column.UINT8, 1},
		{column.UINT16, 2},
		{column.UINT, 4},
		{column.UINT32, 4},
		{column.UINT64, 8},
		{column.FLOAT32, 4},
		{column.FLOAT64, 8},
	}

	for _, val := range values {
		if size := column.GetTypeSize(val.given); size != val.wantSize {
			t.Errorf("Expected size: %d for type %d but got size: %d", val.wantSize, val.given, size)
		}
	}
}

func TestNewVarLengthRecordWithHDR(t *testing.T) {
	type valType struct {
		given         []byte
		wantNullField NullField_T
		wantLocation  []LocationPair
		wantField     []byte
	}
	values := []valType{
		{
			given:         []byte("1\n0,2:2,3:5,2\nitwasyou"),
			wantNullField: 1,
			wantLocation:  []LocationPair{{0, 2}, {2, 3}, {5, 2}},
			wantField:     []byte("itwasyou"),
		},
		{
			given:         []byte("15\n10,2:12,3:15,2\n12:34:56:itwasyou"),
			wantNullField: 15,
			wantLocation:  []LocationPair{{10, 2}, {12, 3}, {15, 2}},
			wantField:     []byte("12:34:56:itwasyou"),
		},
	}

	for _, value := range values {
		record, err := NewVarLengthRecordWithHDR(value.given)
		if err != nil {
			t.Errorf("Create record error: %v", err)
		}

		if record.nullField != value.wantNullField {
			t.Errorf("Expected nullField to be %d but got %d", 4, record.nullField)
		}

		for i := 0; i < len(value.wantLocation); i++ {
			loc := value.wantLocation[i]
			if loc.offset != record.location[i].offset || loc.size != record.location[i].size {
				t.Errorf("Expected offset: %d and size %d", loc.offset, loc.size)
			}
		}

		if !bytes.Equal(record.field, value.wantField) {
			t.Errorf("Field should be equal: %s", record.field)
		}
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

func TestGetField(t *testing.T) {
	type valType struct {
		given      []byte
		givenField string
		wantValue  []byte
	}
	data := []byte("127\n14,2:16,3:19,2\n12:34:1467:56\nitwasyou")
	values := []valType{
		{
			given:      data,
			givenField: "field1",
			wantValue:  []byte("12"),
		},
		{
			given:      data,
			givenField: "field2",
			wantValue:  []byte("34"),
		},
		{
			given:      data,
			givenField: "field3",
			wantValue:  []byte("1467"),
		},
		{
			given:      data,
			givenField: "field4",
			wantValue:  []byte("56"),
		},
		{
			given:      data,
			givenField: "field5",
			wantValue:  []byte("it"),
		},
		{
			given:      data,
			givenField: "field6",
			wantValue:  []byte("was"),
		},
		{
			given:      data,
			givenField: "field8",
			wantValue:  nil,
		},
	}

	colData := NewColumnData()
	for _, val := range values {
		record, err := NewVarLengthRecordWithHDR(val.given)
		if err != nil {
			t.Errorf("%v", err)
		}
		if fieldVal := record.GetField(colData, val.givenField); !bytes.Equal(fieldVal, val.wantValue) {
			t.Errorf("Expected %s but found %s", val.wantValue, fieldVal)
		}
	}
}

func TestUpdateField(t *testing.T) {
	type valType struct {
		givenData     []byte
		givenField    string
		givenValue    []byte
		wantData      []byte
		wantValue     []byte
		wantNullField NullField_T
		wantLocation  []LocationPair
	}
	values := []valType{
		{
			givenData:     []byte("127\n14,2:16,3:19,3\n12:34:1467:56\nitwasyou"),
			givenField:    "field3",
			givenValue:    []byte("146"),
			wantValue:     []byte("146"),
			wantData:      []byte("12:34:146:56\nitwasyou"),
			wantNullField: 127,
			wantLocation: []LocationPair{
				{14, 2}, {16, 3}, {19, 3},
			},
		},
		{
			givenData:     []byte("127\n14,2:16,3:19,3\n12:34:1467:56\nitwasyou"),
			givenField:    "field4",
			givenValue:    []byte(""),
			wantValue:     []byte(""),
			wantData:      []byte("12:34:1467:\nitwasyou"),
			wantNullField: 119,
			wantLocation: []LocationPair{
				{14, 2}, {16, 3}, {19, 3},
			},
		},
		{
			givenData:     []byte("127\n14,2:16,3:19,3\n12:34:1467:56\nitwasyou"),
			givenField:    "field2",
			givenValue:    []byte("900000"),
			wantValue:     []byte("900000"),
			wantData:      []byte("12:900000:1467:56\nitwasyou"),
			wantNullField: 127,
			wantLocation: []LocationPair{
				{14, 2}, {16, 3}, {19, 3},
			},
		},
		{
			givenData:     []byte("127\n13,2:15,3:18,3\n12:34:146:56\nitwasyou"),
			givenField:    "field5",
			givenValue:    []byte("she"),
			wantValue:     []byte("she"),
			wantData:      []byte("12:34:146:56\nshewasyou"),
			wantNullField: 127,
			wantLocation: []LocationPair{
				{13, 3}, {16, 3}, {19, 3},
			},
		},
		{
			givenData:     []byte("127\n13,2:15,3:18,3\n12:34:146:56\nitwasyou"),
			givenField:    "field7",
			givenValue:    []byte("he"),
			wantValue:     []byte("he"),
			wantData:      []byte("12:34:146:56\nitwashe"),
			wantNullField: 127,
			wantLocation: []LocationPair{
				{13, 2}, {15, 3}, {18, 2},
			},
		},
	}
	colData := NewColumnData()

	for _, val := range values {
		record, err := NewVarLengthRecordWithHDR(val.givenData)
		if err != nil {
			t.Errorf("%v", err)
		}
		record.UpdateField(colData, val.givenField, val.givenValue)
		if fieldVal := record.GetField(colData, val.givenField); !bytes.Equal(fieldVal, val.wantValue) {
			t.Errorf("Expected %s but found %s", val.wantValue, fieldVal)
		}
		if !bytes.Equal(record.field, val.wantData) {
			t.Errorf("Expected data: %s \n\nbut found data: %s", val.wantData, record.field)
		}
		if record.nullField != val.wantNullField {
			t.Errorf("Expected nullField: %d but found nullfield: %d", val.wantNullField, record.nullField)
		}
		for i, loc := range val.wantLocation {
			if loc.offset != record.location[i].offset || loc.size != record.location[i].size {
				t.Errorf("Expected offset: %d and size %d\nbut found offset: %d and size %d", loc.offset, loc.size, record.location[i].offset, record.location[i].size)
			}
		}
	}
}

func TestNewVarLengthRecord(t *testing.T) {
	type valType struct {
		given      [][]byte
		wantRecord VarLengthRecord
	}

	values := []valType{
		{
			given: [][]byte{[]byte("12"), []byte("23846"), []byte("-983738"), []byte("83456"), []byte("Hello World")},
			wantRecord: VarLengthRecord{
				recordHeader: recordHeader{nullField: 31, location: []LocationPair{{Location_T(0), Location_T(11)}}},
				field:        []byte("12:23846:-983738:83456\nHello World"),
			},
		},
		{
			given: [][]byte{[]byte("12"), []byte("23846"), []byte("-983738"), []byte(""), []byte("Hello World"), []byte("Power to the People")},
			wantRecord: VarLengthRecord{
				recordHeader: recordHeader{nullField: 55, location: []LocationPair{{0, 11}, {12, 19}}},
				field:        []byte("12:23846:-983738:\nHello WorldPower to the People"),
			},
		},
	}

	for _, val := range values {
		cols := NewColumnData()
		record, err := NewVarLengthRecord(cols.keys, val.given)
		if err != nil {
			t.Error(err)
		}
		if record.nullField != val.wantRecord.nullField {
			t.Errorf("Expected nullField to be %d but got %d", val.wantRecord.nullField, record.nullField)
		}

		for i := 0; i < len(val.wantRecord.location); i++ {
			loc := val.wantRecord.location[i]
			if loc.offset != record.location[i].offset || loc.size != record.location[i].size {
				t.Errorf("Expected offset: %d and size %d\nbut got offset: %d and size %d", loc.offset, loc.size, record.location[i].offset, record.location[i].size)
			}
		}

		if !bytes.Equal(record.field, val.wantRecord.field) {
			t.Errorf("Expected field: %s but got %s", val.wantRecord.field, record.field)
		}
	}
}

func TestToByte(t *testing.T) {
	wantData := []byte("127\n14,2:16,3:19,3\n12:34:1467:56\nitwasyou")
	// wantByte := 41
	givenData := recordHeader{
		nullField: NullField_T(127),
		location: []LocationPair{
			{14, 2}, {16, 3}, {19, 3},
		},
	}
	record := VarLengthRecord{
		recordHeader: givenData,
		field:        []byte("12:34:1467:56\nitwasyou"),
	}

	toByte := record.ToByte()
	if !bytes.Equal(toByte, wantData) {
		t.Errorf("Bytes value did not match. Expected: \n%s \n\nbut got\n \n%s", wantData, toByte)
	}

}

func TestRecordSize(t *testing.T) {
	wantSize := 41
	givenData := recordHeader{
		nullField: NullField_T(127),
		location: []LocationPair{
			{14, 2}, {16, 3}, {19, 3},
		},
	}
	record := VarLengthRecord{
		recordHeader: givenData,
		field:        []byte("12:34:1467:56\nitwasyou"),
	}
	sz := record.RecordSize()
	if sz != wantSize {
		t.Errorf("Record size: expected \n%d \n\nbut got\n \n%d", wantSize, sz)
	}
}
