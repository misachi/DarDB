package db

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"

	st "github.com/misachi/DarDB/storage"
	row "github.com/misachi/DarDB/storage/db/row"
	// db "github.com/misachi/DarDB/storage/database"
)

var (
	ErrBlockFull = errors.New("Block is full")
)

const BLKSIZE = 4096 // Size of block on disk
type blk_t uint64

type BlockLocationPair struct {
	*row.LocationPair
	lockField *st.Lock
}

func NewBlockLocationPair(offset, size st.Location_T) *BlockLocationPair {
	// locationPair := &NewLocationPair(offset, size),
	return &BlockLocationPair{
		LocationPair: row.NewLocationPair(offset, size),
	}
}

type Block struct {
	isDirty     bool
	pinCount    int
	size        int // Current size of bloc contents on storage device
	blockId     blk_t
	tblId       tbl_t
	mut         *sync.RWMutex
	recLocation []BlockLocationPair // Contains list of two items (Record offset, Record size)
	records     []byte
}

func NewBlock(data []byte, blkID blk_t, tblId tbl_t) (*Block, error) {
	if len(data) < 1 {
		return new(Block), nil
	}
	copyData := make([]byte, len(data))
	copy(copyData, data)
	szOffset := bytes.IndexByte(copyData, row.Term)

	if szOffset < 0 {
		szOffset = 0
	}
	locOffset := bytes.IndexByte(copyData[szOffset+1:], row.Term)
	if locOffset < 0 {
		locOffset = 0
	}

	records := copyData[locOffset+1:]
	reader := bytes.NewReader(copyData[:szOffset])
	sz, err := row.ByteArrayToInt(reader)
	if err != nil {
		return nil, fmt.Errorf("NewBlock: byte slice to integer %v", err)
	}

	locations, err := setBlockLocation(copyData[szOffset+1 : locOffset+szOffset+1])
	if err != nil {
		return nil, fmt.Errorf("NewBlock: unable to set location data %v", err)
	}

	return &Block{
		size:        int(sz),
		recLocation: *locations,
		records:     records,
		mut:         &sync.RWMutex{},
		blockId:     blkID,
		tblId:       tblId,
		// lockField:   make([]uint8, len(*locations)),
	}, nil
}

func setBlockLocation(lData []byte) (*[]BlockLocationPair, error) {
	bufSize := len(lData)
	newBuf := make([]byte, bufSize)
	var location []BlockLocationPair

	if numCopy := copy(newBuf, lData); numCopy != bufSize {
		return nil, fmt.Errorf("setLocation copy error: expected to copy %d elements but got %d", bufSize, numCopy)
	}
	locSep := ':'   // Location separator
	fieldSep := ',' // Separator between offset and size
	idx := 0

	for len(newBuf) > 0 {
		locSepIdx := bytes.IndexByte(newBuf, byte(locSep))
		fieldSepIdx := bytes.IndexByte(newBuf, byte(fieldSep))

		if fieldSepIdx == -1 && locSepIdx == -1 {
			break
		}

		if locSepIdx == -1 {
			locSepIdx = len(newBuf)
		}

		offset, err := row.ByteArrayToInt(bytes.NewReader(newBuf[:fieldSepIdx]))
		if err != nil {
			return nil, fmt.Errorf("setLocation: Unable to set offset: %v", err)
		}

		size, err := row.ByteArrayToInt(bytes.NewReader(newBuf[fieldSepIdx+1 : locSepIdx]))
		if err != nil {
			return nil, fmt.Errorf("setLocation: Unable to set size: %v", err)
		}

		if size > 0 {
			if len(location) <= 0 {
				location = []BlockLocationPair{*NewBlockLocationPair(st.Location_T(offset), st.Location_T(size))}
			} else {
				location = append(location, *NewBlockLocationPair(st.Location_T(offset), st.Location_T(size)))
			}
		}

		idx += 1
		if (locSepIdx + 1) > len(newBuf) {
			break
		}
		newBuf = newBuf[locSepIdx+1:]
	}
	return &location, nil
}

func NewBlockWithHDR(data []byte) (*Block, error) {
	var recordSep byte = '\t'
	if len(data) < 1 {
		return new(Block), nil
	}
	copyData := make([]byte, len(data))
	copy(copyData, data)
	idOffset := bytes.IndexByte(copyData, row.Term)
	blkID, err := row.ByteArrayToInt(bytes.NewReader(copyData[:idOffset]))

	if err != nil {
		return nil, fmt.Errorf("NewBlockWithHDR: reading block ID %v", err)
	}
	// szOffset := bytes.IndexByte(copyData, Term)
	// if szOffset < 0 {
	// 	szOffset = 0
	// }

	copyData = copyData[idOffset+1:]
	locEnd := bytes.IndexByte(copyData, recordSep)
	locations, err := setBlockLocation(copyData[:locEnd])
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
	szEnd := bytes.IndexByte(copyData, row.Term)
	sz, err := row.ByteArrayToInt(bytes.NewReader(copyData[:szEnd]))
	if err != nil {
		return nil, fmt.Errorf("NewBlock: reading block size: %v", err)
	}

	return &Block{
		size:        int(sz),
		recLocation: *locations,
		records:     records,
		mut:         new(sync.RWMutex),
		blockId:     blk_t(blkID),
	}, nil
}

func (b Block) BlockID() blk_t {
	return b.blockId
}

func intToByte(i int) []byte {
	return []byte(strconv.Itoa(int(i)))
}

func (b *Block) ToByte() []byte {
	var recordSep byte = '\t'
	retData := intToByte(int(b.blockId))
	retData = append(retData, row.Term)
	locSize := len(b.recLocation)

	for i, location := range b.recLocation {
		retData = append(retData, intToByte(int(location.Offset()))...)
		retData = append(retData, row.LocationSep)
		retData = append(retData, intToByte(int(location.Size()))...)

		if i < (locSize - 1) {
			retData = append(retData, row.FieldSep)
		}
	}
	retData = append(retData, recordSep)

	retData = append(retData, b.records...)
	retData = append(retData, recordSep)
	retData = append(retData, intToByte(b.size)...)
	retData = append(retData, row.Term)
	return retData
}

func (b *Block) AddRecordWithBytes(data []byte) error {
	record, err := row.NewVarLengthRecordWithHDR(data)

	if err != nil {
		return fmt.Errorf("AddRecord: %v", err)
	}

	length := record.RecordSize()
	if b.size > BLKSIZE || (b.size+length) > BLKSIZE {
		return fmt.Errorf("AddRecord: Block is full")
	}

	offset := len(b.records)
	locationPair := NewBlockLocationPair(st.Location_T(offset), st.Location_T(length))
	b.recLocation = append(b.recLocation, *locationPair)
	b.records = append(b.records, data...)
	b.size += length
	b.isDirty = true
	return nil
}

func (b *Block) AddRecord(record *row.VarLengthRecord) error {
	length := record.RecordSize()

	if b.size > BLKSIZE || (b.size+length) > BLKSIZE {
		return fmt.Errorf("AddRecord: Block is full")
	}

	offset := len(b.records)
	locationPair := NewBlockLocationPair(st.Location_T(offset), st.Location_T(length))
	b.recLocation = append(b.recLocation, *locationPair)
	b.records = append(b.records, record.ToByte()...)
	b.size += length
	b.isDirty = true
	return nil
}

func (b *Block) getRecordSlice(offset, size int) (row.Record, error) {
	return row.NewVarLengthRecordWithHDR(b.records[offset : offset+size])
}

func (b *Block) Records(ctx *ClientContext) ([]row.Record, error) {
	filtered := make([]row.Record, 0)
	for _, location := range b.recLocation {
		location.lockField.AcquireLock(st.SHARED_LOCK)
		record, err := b.getRecordSlice(int(location.Offset()), int(location.Size()))
		if err != nil {
			return nil, fmt.Errorf("Records: Unable to initialize record %v", err)
		}
		filtered = append(filtered, record)
	}
	return filtered, nil
}

func (b Block) FilterRecords(ctx *ClientContext, colData row.ColumnData, fieldName string, fieldVal []byte) ([]row.Record, error) {
	filtered := make([]row.Record, 0)

	for i, _ := range b.recLocation {
		b.recLocation[i].lockField.AcquireLock(st.SHARED_LOCK)
		if b.recLocation[i].Offset() >= 0 && b.recLocation[i].Size() > 0 {

			record, err := b.getRecordSlice(int(b.recLocation[i].Offset()), int(b.recLocation[i].Size()))

			if err != nil {
				return nil, fmt.Errorf("FilterRecords: Unable to initialize record %v", err)
			}
			if field := record.GetField(colData, fieldName); bytes.Equal(field, fieldVal) {
				filtered = append(filtered, record)
			}
		}
	}
	return filtered, nil
}

func (b *Block) UpdateFiteredRecords(ctx *ClientContext, colData row.ColumnData, fieldName string, searchVal []byte, newVal []byte) error {
	// colData := NewColumnData()
	for _, location := range b.recLocation {
		location.lockField.AcquireLock(st.EXCLUSIVE_LOCK)
		record, err := b.getRecordSlice(int(location.Offset()), int(location.Size()))

		if err != nil {
			return fmt.Errorf("UpdateFiteredRecords: Unable to initialize record %v", err)
		}
		if field := record.GetField(colData, fieldName); bytes.Equal(field, searchVal) {
			b.size -= record.(*row.VarLengthRecord).RecordSize()
			record.UpdateField(colData, fieldName, newVal)
			b.size += record.(*row.VarLengthRecord).RecordSize()
		}
	}

	b.isDirty = true
	return nil
}

func (b *Block) UpdateRecords(ctx *ClientContext, colData row.ColumnData, fieldName string, fieldVal []byte) error {
	for _, location := range b.recLocation {
		location.lockField.AcquireLock(st.EXCLUSIVE_LOCK)
		record, err := b.getRecordSlice(int(location.Offset()), int(location.Size()))

		if err != nil {
			return fmt.Errorf("UpdateRecords: Unable to initialize record %v", err)
		}

		b.size -= record.(*row.VarLengthRecord).RecordSize()
		record.UpdateField(colData, fieldName, fieldVal)
		b.size += record.(*row.VarLengthRecord).RecordSize()
	}
	b.isDirty = true
	return nil
}
