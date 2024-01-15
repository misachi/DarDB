package db

import (
	"fmt"
	"log/slog"
	"math"
	"reflect"

	dsk "github.com/misachi/DarDB/storage"
	ds "github.com/misachi/DarDB/structure"
)

type Pool interface {
	Pop() interface{}
	Get(key interface{}) interface{}
	Push(_key interface{}, _data interface{})
	Remove(key interface{})
	Head() interface{}
}

var bufMgr *BufferPoolMgr

const (
	ALIGN      = BLKSIZE
	ALIGN_MASK = (ALIGN - 1)
)

func isAligned(val int64) bool {
	return (val & ALIGN_MASK) == 0
}

func alignBlock(sz int64) int64 {
	if isAligned(sz) {
		return sz
	} else {
		return ((sz + ALIGN_MASK) & ^ALIGN_MASK)
	}
}

func GetBufMgr() *BufferPoolMgr {
	if bufMgr != nil {
		buf, _ := NewBufferPoolMgr()
		return buf
	}
	return bufMgr
}

type BufferPoolMgr struct {
	blkCount int64 // number of blocks
	block    Pool
	// freeList Pool
}

func NewBufferPoolMgr() (*BufferPoolMgr, error) {
	if bufMgr != nil {
		return bufMgr, nil
	}
	// mgr, err := dsk.NewDiskMgr(tblID)
	// if err != nil {
	// 	return nil, fmt.Errorf("NewBufferPoolMgr: Unable to create new file manager %v", err)
	// }
	bufMgr = &BufferPoolMgr{
		blkCount:    0, // int64(math.Ceil(float64(alignBlock(mgr.Size()))/BLKSIZE)),
		block:       ds.NewList(),
	}
	return bufMgr, nil
}

func NewInternalBufferPoolMgr(psize int64, fName string) (*BufferPoolMgr, error) {
	mgr, err := dsk.NewDiskMgr(fName)
	if err != nil {
		return nil, fmt.Errorf("NewInternalBufferPoolMgr: Unable to create new file manager %v", err)
	}
	bufMgr = &BufferPoolMgr{
		blkCount:    int64(math.Ceil(float64(alignBlock(mgr.Size()))/BLKSIZE)),
		block:       ds.NewList(),
	}

	return bufMgr, nil
}

func (buf *BufferPoolMgr) Load(tblID dsk.Tbl_t, loc string) error {
	mgr, err := dsk.NewDiskMgr(loc)
	if err != nil {
		return fmt.Errorf("Load: Unable to create new file manager %v", err)
	}
	fData := make([]byte, mgr.Size())

	if _, err := mgr.Read(fData); err != nil {
		return fmt.Errorf("Load: error reading file %v", err)
	}

	blockID := 1
	for {
		if len(fData) < 1 {
			break
		}
		blk, err := NewBlock(fData[:BLKSIZE], dsk.Blk_t(blockID), tblID)
		buf.blkCount += 1
		if err != nil {
			return fmt.Errorf("Load: error creating new block %v", err)
		}
		key := fmt.Sprintf("%d_%d", tblID, blk.blockId)
		buf.block.Push(key, blk)
		fData = fData[BLKSIZE:]
		blockID += 1
	}
	return nil
}

func (buf *BufferPoolMgr) NumBlocks() int64 {
	return buf.blkCount
}

func (buf *BufferPoolMgr) AddBlockToPool(key string, blk *Block) {
	buf.blkCount += 1
	buf.block.Push(key, blk)
}

func (buf *BufferPoolMgr) GetBlock(path string, tblId dsk.Tbl_t, blockId dsk.Blk_t) (*Block, error) {
	key := fmt.Sprintf("%d_%d", tblId, blockId)

	if blk := buf.block.Get(key); !reflect.ValueOf(blk.(*Block)).IsNil() {
		return blk.(*Block), nil
	}

	mgr, err := dsk.NewDiskMgr(path)
	if err != nil {
		return nil, fmt.Errorf("GetBlock: Unable to create new disk manager %v", err)
	}

	_, err = mgr.Seek(int64(blockId), 0)
	if err != nil {
		return nil, fmt.Errorf("GetBlock: Seek error %v", err)
	}
	blkData := make([]byte, BLKSIZE)
	_, err = mgr.Read(blkData)
	if err != nil {
		return nil, fmt.Errorf("GetBlock: Read error %v", err)
	}
	blk, err := NewBlockWithHDR(blkData)
	if err != nil {
		return nil, fmt.Errorf("GetBlock: new block error %v", err)
	}
	blk.tblId = tblId
	buf.AddBlockToPool(key, blk)
	return blk, nil
}

func (buf *BufferPoolMgr) GetFree(path string, tblId dsk.Tbl_t, sz int) *Block {
	next := buf.block.Head()
	for !reflect.ValueOf(next.(*ds.Value)).IsNil() {
		key := fmt.Sprintf("%d_%d", tblId, next.(*ds.Value).Data().(*Block).blockId)
		if next.(*ds.Value).Data().(*Block).Size() >= sz && reflect.ValueOf(next.(*ds.Value).Key()).String() == key {
			return next.(*ds.Value).Data().(*Block)
		}
		next = next.(*ds.Value).Next()
	}

	mgr, err := dsk.NewDiskMgr(path)
	if err != nil {
		slog.Warn("GetFree: Unable to create new disk manager %v", err)
		return nil
	}

	fileSize := mgr.Size()
	newBlkID := (math.Ceil(float64(fileSize)/BLKSIZE) * BLKSIZE) + 1
	blk, err := NewBlock(make([]byte, 0), dsk.Blk_t(newBlkID), tblId)
	if err != nil {
		slog.Warn("GetFree: Unable to create new block %v", err)
		return nil
	}

	key := fmt.Sprintf("%d_%d", tblId, blk.blockId)
	blk.tblId = tblId
	buf.AddBlockToPool(key, blk)
	return blk
}

func (buf *BufferPoolMgr) writeBlock(path string, blk *Block) {
	mgr, err := dsk.NewDiskMgr(path)
	if err != nil {
		panic(fmt.Sprintf("flushBlock: Unable to create new disk manager: %v", err))
	}

	if _, err := mgr.Seek(int64(blk.blockId), 0); err != nil {
		panic(fmt.Sprintf("flushBlock Seek: %v", err))
	}

	if _, err = mgr.Write(blk.ToByte()); err != nil {
		panic(fmt.Sprintf("flushBlock Write: %v", err))
	}

	blk.ResetIsDirtyFlag()
}

func (buf *BufferPoolMgr) WriteBlock(path string, tblId dsk.Tbl_t, blockID dsk.Blk_t) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("WriteBlock error: %v", r)
		}
	}()

	key := fmt.Sprintf("%d_%d", tblId, blockID)
	blk := buf.block.Get(key)
	if blk != nil {
		buf.writeBlock(path, blk.(*Block))
	}
}

func (buf *BufferPoolMgr) Flush(path string, tblId dsk.Tbl_t) error {
	mgr, err := dsk.NewDiskMgr(path)
	if err != nil {
		return fmt.Errorf("BufferPoolMgr Flush: Unable to create new disk manager: %v", err)
	}

	if err := mgr.Flush(); err != nil {
		return fmt.Errorf("BufferPoolMgr Flush error: %v", err)
	}
	return nil
}


type BufferPoolMgr2 struct {
	poolLength  uint
	poolSize    int64 // number of blocks
	tblID       dsk.Tbl_t
	diskManager *dsk.DiskMgr
	block       Pool
	freeList    Pool
}

func NewBufferPoolMgr2(psize int64, fName string, tblID dsk.Tbl_t) (*BufferPoolMgr, error) {
	if bufMgr != nil {
		return bufMgr, nil
	}
	mgr, err := dsk.NewDiskMgr(fName)
	if err != nil {
		return nil, fmt.Errorf("NewBufferPoolMgr: Unable to create new manager %v", err)
	}
	bufMgr = &BufferPoolMgr{
		blkCount:    int64(math.Ceil(float64(alignBlock(mgr.Size()))/BLKSIZE)), //  alignBlock(mgr.Size()) / BLKSIZE,
		// diskManager: mgr,
		block:       ds.NewList(),
		// freeList:    ds.NewList(),
		// tblID:       tblID,
	}
	return bufMgr, nil
}

func NewInternalBufferPoolMgr2(psize int64, fName string) (*BufferPoolMgr, error) {
	mgr, err := dsk.NewDiskMgr(fName)
	if err != nil {
		return nil, fmt.Errorf("BufferPoolMgr: Unable to create new manager %v", err)
	}
	bufMgr = &BufferPoolMgr{
		blkCount:    int64(math.Ceil(float64(alignBlock(mgr.Size()))/BLKSIZE)),
		// diskManager: mgr,
		block:       ds.NewList(),
		// freeList:    ds.NewList(),
	}

	return bufMgr, nil
}

func (buf *BufferPoolMgr2) NumBlocks() int64 {
	return buf.poolSize
}

func (buf *BufferPoolMgr2) Load() error {
	fData := make([]byte, buf.diskManager.Size())
	_, err := buf.diskManager.Read(fData)
	if err != nil {
		return fmt.Errorf("Load: error reading file %v", err)
	}

	blockID := 1
	for {
		if len(fData) < 1 {
			break
		}
		blk, err := NewBlock(fData[:BLKSIZE], dsk.Blk_t(blockID), buf.tblID)
		buf.poolSize += 1
		if err != nil {
			return fmt.Errorf("Load: error creating new block %v", err)
		}
		buf.block.Push(int64(blk.blockId), blk)
		fData = fData[BLKSIZE:]
		blockID += 1
	}
	return nil
}

func (buf *BufferPoolMgr2) GetBlock(blockId int64) (*Block, error) {
	if blk := buf.block.Get(blockId); blk != nil {
		return blk.(*Block), nil
	}
	_, err := buf.diskManager.Seek(int64(blockId), 0)
	if err != nil {
		return nil, fmt.Errorf("GetBlock: Seek error %v", err)
	}
	blkData := make([]byte, BLKSIZE)
	_, err = buf.diskManager.Read(blkData)
	if err != nil {
		return nil, fmt.Errorf("GetBlock: Read error %v", err)
	}
	blk, err := NewBlockWithHDR(blkData)
	if err != nil {
		return nil, fmt.Errorf("GetBlock: new block error %v", err)
	}
	buf.block.Push(int64(blk.blockId), blk)
	return blk, nil
}

func (buf *BufferPoolMgr2) GetFree(sz int) *Block {
	blk := buf.block.Head()
	if blk == nil {
		blk = buf.freeList.Head()
	}

	if reflect.ValueOf(blk.(*ds.Value)).IsNil() {
		data := make([]byte, 0)
		fileSize := buf.diskManager.Size()
		if buf.NumBlocks() <= 0 {
			_blk, err := NewBlock(data, dsk.Blk_t(fileSize), buf.tblID)
			if err != nil {
				return nil
			}
			buf.poolSize += 1
			buf.block.Push(fileSize, _blk)
			return _blk
		} else {
			blkID := buf.NumBlocks() - 1
			_blk, err := buf.GetBlock(blkID)
			if err != nil {
				return nil
			}
			buf.block.Push(fileSize, _blk)
			return _blk
		}
	}
	if blk != nil && blk.(*ds.Value).Data().(*Block).size > sz {
		return blk.(*ds.Value).Data().(*Block)
	}

	return nil
}

func (buf *BufferPoolMgr2) flushBlock(blockID int, blk *Block) error {
	_, err := buf.diskManager.Seek(int64(blockID), 0)
	if err != nil {
		return fmt.Errorf("flushBlock Seek: %v", err)
	}

	_, err = buf.diskManager.Write(blk.ToByte())
	if err != nil {
		return fmt.Errorf("flushBlock Write: %v", err)
	}

	buf.diskManager.Flush()
	buf.freeList.Push(int64(blockID), blk)
	return nil
}

func (buf *BufferPoolMgr2) FlushBlock(blockID int) {
	blk := buf.block.Get(int64(blockID))
	if blk != nil {
		buf.flushBlock(blockID, blk.(*Block))
	}
}

// func (buf *BufferPoolMgr) Flush() error {
// 	allBlks := make([]byte, buf.poolSize * BLKSIZE)
// 	blk := buf.block.Head()
// 	// for blk != nil {
// 	// 	blk = blk.
// 	// }
// 	i := 0
// 	for buf.poolSize > int64(i) && blk != nil {
// 		allBlks = append(allBlks, )
// 		i += 1
// 	}
// 	return nil
// }
