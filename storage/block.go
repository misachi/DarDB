package storage

import (
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
	offset      uint           // start of block on file
	size        uint16         // Size of records(in bytes)
	numRecord   uint16         // Number of records in block
	recLocation []LocationPair // Contains list of two items (Record offset, Record size)
	record      []Record
}

func NewBlock(data []byte) *Block {
	if len(data) < 1 {
		return &Block{}
	}
	return &Block{}
}

func (b *Block) Size() uint16      { return b.size }
func (b *Block) NumRecord() uint16 { return b.numRecord }

// func (b *Block) Location(idx uint16) LocationPair { return b.recLocation[idx] }
func (b *Block) addRecord(data []byte) {
	offset := len(b.record)
	length := len(data)
	locationPair := NewLocationPair(Location_T(offset), Location_T(length))
	b.recLocation = append(b.recLocation, *locationPair)
}
func (b *Block) Get(keys ...interface{}) {
	// var record Record
	// found := false
	// for _, loc := range b.entryLocation {

	// }
}
func (b *Block) SetNumEntry() {
	b.numRecord += uint16(len(b.recLocation))
}
func (b *Block) SetSize(size uint16) error {
	if err := b.checkSize(size); err != nil {
		return fmt.Errorf("SetSize: %w", err)
	}
	b.size += size
	return nil
}
func (b *Block) checkSize(size uint16) error {
	if b.size >= BLKSIZE {
		return ErrBlockFull
	}

	if (BLKSIZE - b.size) < size {
		return ErrBlockFull
	}

	if sz := b.size + size; sz > BLKSIZE {
		return ErrBlockFull
	}
	return nil
}
func (b *Block) Add(data []byte) error {
	if err := b.SetSize(uint16(len(data))); err != nil {
		return err
	}
	b.addRecord(data)
	b.SetNumEntry()
	b.record = append(b.record) // data...)
	return nil
}

type BlockIterator struct {
	data    *[]byte      // Underlying Block contents
	current LocationPair // Offset and size of current record
}

func (I *BlockIterator) Next()             {}
func (I *BlockIterator) Prev()             {}
func (I *BlockIterator) Seek(target int32) {}
