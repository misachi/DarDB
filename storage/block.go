package storage

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	ErrBlockFull = errors.New("Block is full")
)

const BLKSIZE = 4096 // Size of block on disk

type Record interface {
	GetField(key string) []byte
	UpdateField(key string, value []byte)
	AddField(key string, value []byte)
}

type Block struct {
	size        int            // Current size of bloc contents on storage device
	recLocation []LocationPair // Contains list of two items (Record offset, Record size)
	records     []byte
}

type BlockMgr struct {
	block      []Block // Blocs in memory
	freeBlocks []Block
}

func NewBlock() *Block {
	return &Block{}
}

func (b *Block) AddRecord(data []byte) error {
	length := len(data)
	if b.size > BLKSIZE || (b.size+length) > BLKSIZE {
		return fmt.Errorf("AddRecord: Block is full")
	}
	offset := len(b.records)
	locationPair := NewLocationPair(Location_T(offset), Location_T(length))
	b.recLocation = append(b.recLocation, *locationPair)
	b.records = append(b.records, data...)
	b.size += length
	return nil
}

func (b Block) getRecordSlice(offset, size int) (Record, error) {
	return NewVarLengthRecordWithHDR(b.records[offset : offset+size])
}

func (b Block) Records() ([]Record, error) {
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

func (b Block) FilterRecords(fieldName string, fieldVal []byte) ([]Record, error) {
	filtered := make([]Record, 0)
	for _, location := range b.recLocation {
		record, err := b.getRecordSlice(int(location.offset), int(location.size))
		if err != nil {
			return nil, fmt.Errorf("FilterRecords: Unable to initialize record %v", err)
		}
		if field := record.GetField(fieldName); bytes.Equal(field, fieldVal) {
			filtered = append(filtered, record)
		}
	}
	return filtered, nil
}

func (b *Block) UpdateFiteredRecords(fieldName string, searchVal []byte, newVal []byte) error {
	for _, location := range b.recLocation {
		record, err := b.getRecordSlice(int(location.offset), int(location.size))
		if err != nil {
			return fmt.Errorf("UpdateFiteredRecords: Unable to initialize record %v", err)
		}
		if field := record.GetField(fieldName); bytes.Equal(field, searchVal) {
			record.UpdateField(fieldName, newVal)
		}
	}
	return nil
}

func (b *Block) UpdateRecords(fieldName string, fieldVal []byte) error {
	for _, location := range b.recLocation {
		record, err := b.getRecordSlice(int(location.offset), int(location.size))
		if err != nil {
			return fmt.Errorf("UpdateRecords: Unable to initialize record %v", err)
		}
		record.UpdateField(fieldName, fieldVal)
	}
	return nil
}
