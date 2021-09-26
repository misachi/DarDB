package storage

import (
	"errors"
	"fmt"

	"github.com/misachi/DarDB/util"
)

var (
	ErrBlockFull = errors.New("Block is full")
)

const BLKSIZE = 8192 // Size of block on disk

type Record interface {
	GetField(idx uint16) util.Pair
	CreateField(fieldName string, value interface{})
	UpdateField(idx uint16, value interface{})
}

func NewLocationPair(offset, size Location_T) *LocationPair {
	return &LocationPair{
		offset: offset,
		size:   size,
	}
}

type Block struct {
	size             uint16         // Size of records(in bytes)
	numEntryLocation uint16         // Number of records in block
	entryLocation    []LocationPair // Contains list of two items (Record offset, Record size)
	blockEntry       []byte
}

func (b *Block) Size() uint16                     { return b.size }
func (b *Block) NumEntry() uint16                 { return b.numEntryLocation }
func (b *Block) Location(idx uint16) LocationPair { return b.entryLocation[idx] }
func (b *Block) addLocationEntry(data []byte) {
	offset := len(b.blockEntry)
	length := len(data)
	locationPair := NewLocationPair(Location_T(offset), Location_T(length))
	b.entryLocation = append(b.entryLocation, *locationPair)
}
func (b *Block) Get(keys ...interface{}) {
	// var record Record
	// found := false
	// for _, loc := range b.entryLocation {

	// }
}
func (b *Block) SetNumEntry() {
	b.numEntryLocation += uint16(len(b.entryLocation))
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
	b.addLocationEntry(data)
	b.SetNumEntry()
	b.blockEntry = append(b.blockEntry, data...)
	return nil
}
func NewBlock() *Block {
	return &Block{}
}

type BlockIterator struct {
	data    *[]byte      // Underlying Block contents
	current LocationPair // Offset and size of current record
}

func (I *BlockIterator) Next()             {}
func (I *BlockIterator) Prev()             {}
func (I *BlockIterator) Seek(target int32) {}
