package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

var (
	ErrBlockFull = errors.New("Block is full")
)

const BLKSIZE = 4096 // Size of block on disk

type Record interface {
	GetField(colData columnData, key string) []byte
	UpdateField(colData columnData, key string, value []byte)
	AddField(colData columnData, key string, value []byte)
}

type Block struct {
	size        int            // Current size of bloc contents on storage device
	recLocation []LocationPair // Contains list of two items (Record offset, Record size)
	records     []byte
}

type blockW struct {
	next  *blockW
	Block *Block
}

func (bw blockW) Next() blockW {
	return *bw.next
}

type BlockMgr struct {
	memBlock  *blockW // Blocks in memory
	freeBlock *blockW // Blocks with free space
}

func createBlockQ(data []byte) (*blockW, error) {
	head := new(blockW)
	var prev *blockW
	var next = head
	for len(data) > 0 {
		sizeIdx := bytes.IndexByte(data, Term)
		size, err := ByteArrayToInt(bytes.NewReader(data[:sizeIdx]))
		if err != nil {
			return nil, fmt.Errorf("createBlockQ error: unable to convert byte array to integer %v", err)
		}
		newBlock, err := NewBlock(data[:size])
		if err != nil {
			return nil, fmt.Errorf("createBlockQ error: %v", err)
		}
		next.Block = newBlock
		if prev != nil {
			prev.next = next
		}
		prev = next
		next = new(blockW)
		data = data[size:]
	}
	return head, nil
}

func load(r io.Reader, b *BlockMgr, limit int) error {
	buf := make([]byte, limit)
	_, err := io.ReadAtLeast(r, buf, 1)
	if err != nil {
		return fmt.Errorf("load error: %v", err)
	}

	curr, err := createBlockQ(buf)
	if err != nil {
		return fmt.Errorf("load error: %v", err)
	}

	if b.memBlock == nil {
		b.memBlock = curr
	} else {
		b.memBlock.next = curr
	}
	return nil
}

func loadAll(r io.Reader, b *BlockMgr) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("loadAll io.ReadAll error: %v", err)
	}

	curr, err := createBlockQ(data)
	if err != nil {
		return fmt.Errorf("loadAll error: %v", err)
	}
	b.memBlock = curr
	return nil
}

func NewBlockMgr(r io.Reader, limit int) (*BlockMgr, error) {

	block := new(BlockMgr)
	var err error

	if limit < 1 {
		err = loadAll(r, block)
	} else {
		err = load(r, block, limit)
	}

	if err != nil {
		return nil, fmt.Errorf("NewBlockMgr error: %v", err)
	}
	return block, nil
}

func (m BlockMgr) BlockW() blockW {
	return *m.memBlock
}

func (m BlockMgr) NumBlocks() int {
	next := m.memBlock
	count := 0
	for next != nil {
		count += 1
		next = next.next
	}
	return count
}

func (m BlockMgr) NumRecords() int {
	next := m.memBlock
	count := 0
	for next != nil {
		count += next.Block.size
		next = next.next
	}
	return count
}

func NewBlock(data []byte) (*Block, error) {
	if len(data) < 1 {
		return &Block{}, nil
	}
	copyData := make([]byte, len(data))
	copy(copyData, data)
	szOffset := bytes.IndexByte(copyData, Term)
	locOffset := bytes.IndexByte(copyData[szOffset+1:], Term)
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
	}, nil
}

func (b *Block) AddRecord(data []byte) error {
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
	colData := NewColumnData()
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

func (b *Block) UpdateFiteredRecords(fieldName string, searchVal []byte, newVal []byte) error {
	colData := NewColumnData()
	for _, location := range b.recLocation {
		record, err := b.getRecordSlice(int(location.offset), int(location.size))
		if err != nil {
			return fmt.Errorf("UpdateFiteredRecords: Unable to initialize record %v", err)
		}
		if field := record.GetField(colData, fieldName); bytes.Equal(field, searchVal) {
			record.UpdateField(colData, fieldName, newVal)
		}
	}
	return nil
}

func (b *Block) UpdateRecords(fieldName string, fieldVal []byte) error {
	colData := NewColumnData()
	for _, location := range b.recLocation {
		record, err := b.getRecordSlice(int(location.offset), int(location.size))
		if err != nil {
			return fmt.Errorf("UpdateRecords: Unable to initialize record %v", err)
		}
		record.UpdateField(colData, fieldName, fieldVal)
	}
	return nil
}
