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
	GetField(idx int16) util.Pair
	CreateField(fieldName string, value interface{})
	UpdateField(idx int16, value interface{})
}

type LocationPair struct {
	offset int16
	size   int16
}

func NewLocationPair(offset, size int16) *LocationPair {
	return &LocationPair{
		offset: offset,
		size: size,
	}
}
func (l LocationPair) Offset() int16 { return l.offset }
func (l LocationPair) Size() int16   { return l.size }

type Block struct {
	size          int16          // Size of block in bytes
	freeSpace     int16          // offset to start of freespace
	numEntry      int16          // Number of records in block
	entryLocation []LocationPair // Contains list of two items (Record offset, Record size)
	blockEntry    []byte
}

func (b *Block) Size() int16                     { return b.size }
func (b *Block) NumEntry() int16                 { return b.numEntry }
func (b *Block) FreeSpace() int16                { return b.freeSpace }
func (b *Block) Location(idx int16) LocationPair { return b.entryLocation[idx] }
func (b *Block) addLocationEntry(data []byte) {
	offset := len(b.blockEntry)
	length := len(data)
	locationPair := NewLocationPair(int16(offset), int16(length))
	b.entryLocation = append(b.entryLocation, *locationPair)
}
func (b *Block) SetSize(size int16) error { 
	if err := b.checkSize(size); err != nil {
		return fmt.Errorf("error SetSize: %v", err)
	}
	rune
	return nil
 }
func (b *Block) checkSize(size int16) error {
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
	if err := b.SetSize(int16(len(data))); err != nil {
		return err
	}
	b.addLocationEntry(data)
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

func (I *BlockIterator) Next()
func (I *BlockIterator) Prev()
func (I *BlockIterator) Seek(target int32)
