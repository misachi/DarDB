package storage

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrBlockFull = errors.New("Block is full")
)

const BLKSIZE = 4096 // Size of block on disk

type Record interface {
	GetField(colData columnData, key string) []byte
	UpdateField(colData columnData, key string, value []byte)
	AddField(colData columnData, key string, value []byte)
	LockRecord(lType int)
	UnLockRecord()
}

type Block struct {
	blockId     int
	isDirty     bool
	pinCount    int
	size        int            // Current size of bloc contents on storage device
	recLocation []LocationPair // Contains list of two items (Record offset, Record size)
	records     []byte
	mut         *sync.RWMutex
}

func NewBlock(data []byte, blkID int) (*Block, error) {
	if len(data) < 1 {
		return new(Block), nil
	}
	copyData := make([]byte, len(data))
	copy(copyData, data)
	szOffset := bytes.IndexByte(copyData, Term)

	if szOffset < 0 {
		szOffset = 0
	}
	locOffset := bytes.IndexByte(copyData[szOffset+1:], Term)
	if locOffset < 0 {
		locOffset = 0
	}

	records := copyData[locOffset+1:]
	reader := bytes.NewReader(copyData[:szOffset])
	sz, err := ByteArrayToInt(reader)
	if err != nil {
		return nil, fmt.Errorf("NewBlock: byte slice to integer %v", err)
	}

	locations, err := setLocation(copyData[szOffset+1 : locOffset+szOffset+1])
	if err != nil {
		return nil, fmt.Errorf("NewBlock: unable to set location data %v", err)
	}

	return &Block{
		size:        int(sz),
		recLocation: *locations,
		records:     records,
		mut:         new(sync.RWMutex),
		blockId:     blkID,
	}, nil
}

func NewBlockWithHDR(data []byte) (*Block, error) {
	var recordSep  byte = '\t'
	if len(data) < 1 {
		return new(Block), nil
	}
	copyData := make([]byte, len(data))
	copy(copyData, data)
	idOffset := bytes.IndexByte(copyData, Term)
	blkID, err := ByteArrayToInt(bytes.NewReader(copyData[:idOffset]))

	if err != nil {
		return nil, fmt.Errorf("NewBlockWithHDR: reading block ID %v", err)
	}
	// szOffset := bytes.IndexByte(copyData, Term)
	// if szOffset < 0 {
	// 	szOffset = 0
	// }
	
	copyData = copyData[idOffset+1:]
	locEnd := bytes.IndexByte(copyData, recordSep)
	locations, err := setLocation(copyData[:locEnd])
	if err != nil {
		return nil, fmt.Errorf("NewBlock: unable to set location data %v", err)
	}

	copyData = copyData[locEnd+1:]
	recEnd := bytes.IndexByte(copyData, recordSep)
	// if locOffset < 0 {
	// 	locOffset = 0
	// }
	records := copyData[:recEnd]
	
	copyData = copyData[recEnd+1:]
	szEnd := bytes.IndexByte(copyData, Term)
	sz, err := ByteArrayToInt(bytes.NewReader(copyData[:szEnd]))
	if err != nil {
		return nil, fmt.Errorf("NewBlock: reading block size: %v", err)
	}

	return &Block{
		size:        int(sz),
		recLocation: *locations,
		records:     records,
		mut:         new(sync.RWMutex),
		blockId:     int(blkID),
	}, nil
}

func (b Block) BlockID() int {
	return b.blockId
}

func (b *Block) ToByte() []byte {
	var recordSep  byte = '\t'
	retData := intToByte(int(b.blockId))
	retData = append(retData, Term)
	locSize := len(b.recLocation)

	for i, location := range b.recLocation {
		retData = append(retData, intToByte(int(location.offset))...)
		retData = append(retData, LocationSep)
		retData = append(retData, intToByte(int(location.size))...)

		if i < (locSize - 1) {
			retData = append(retData, FieldSep)
		}
	}
	retData = append(retData, recordSep)

	retData = append(retData, b.records...)
	retData = append(retData, recordSep)
	retData = append(retData, intToByte(b.size)...)
	retData = append(retData, Term)
	return retData
}

func (b *Block) AddRecordWithBytes(data []byte) error {
	record, err := NewVarLengthRecordWithHDR(data)

	if err != nil {
		return fmt.Errorf("AddRecord: %v", err)
	}

	length := record.RecordSize()
	if b.size > BLKSIZE || (b.size+length) > BLKSIZE {
		return fmt.Errorf("AddRecord: Block is full")
	}

	offset := len(b.records)
	locationPair := NewLocationPair(Location_T(offset), Location_T(length))
	b.recLocation = append(b.recLocation, *locationPair)
	b.records = append(b.records, data...)
	b.size += length
	b.isDirty = true
	return nil
}

func (b *Block) getRecordSlice(offset, size int) (Record, error) {
	return NewVarLengthRecordWithHDR(b.records[offset : offset+size])
}

func (b *Block) Records() ([]Record, error) {
	filtered := make([]Record, 0)
	for _, location := range b.recLocation {
		record, err := b.getRecordSlice(int(location.offset), int(location.size))
		if err != nil {
			return nil, fmt.Errorf("Records: Unable to initialize record %v", err)
		}
		filtered = append(filtered, record)
	}
	return filtered, nil
}

func (b Block) FilterRecords(colData columnData, fieldName string, fieldVal []byte) ([]Record, error) {
	filtered := make([]Record, 0)
	for _, location := range b.recLocation {
		record, err := b.getRecordSlice(int(location.offset), int(location.size))
		if err != nil {
			return nil, fmt.Errorf("FilterRecords: Unable to initialize record %v", err)
		}
		if field := record.GetField(colData, fieldName); bytes.Equal(field, fieldVal) {
			filtered = append(filtered, record)
		}
	}
	return filtered, nil
}

func (b *Block) UpdateFiteredRecords(colData columnData, fieldName string, searchVal []byte, newVal []byte) error {
	// colData := NewColumnData()
	for _, location := range b.recLocation {
		record, err := b.getRecordSlice(int(location.offset), int(location.size))
		if err != nil {
			return fmt.Errorf("UpdateFiteredRecords: Unable to initialize record %v", err)
		}
		if field := record.GetField(colData, fieldName); bytes.Equal(field, searchVal) {
			b.size -= record.(*VarLengthRecord).RecordSize()
			record.UpdateField(colData, fieldName, newVal)
			b.size += record.(*VarLengthRecord).RecordSize()
		}
	}
	b.isDirty = true
	return nil
}

func (b *Block) UpdateRecords(colData columnData, fieldName string, fieldVal []byte) error {
	// colData := NewColumnData()
	for _, location := range b.recLocation {
		record, err := b.getRecordSlice(int(location.offset), int(location.size))
		if err != nil {
			return fmt.Errorf("UpdateRecords: Unable to initialize record %v", err)
		}
		b.size -= record.(*VarLengthRecord).RecordSize()
		record.UpdateField(colData, fieldName, fieldVal)
		b.size += record.(*VarLengthRecord).RecordSize()
	}
	b.isDirty = true
	return nil
}
