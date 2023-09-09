package db

import (
	"fmt"
	"reflect"

	ds "github.com/misachi/DarDB/structure"
	dsk "github.com/misachi/DarDB/storage"
)

type Pool interface {
	Pop() interface{}
	Get(key interface{}) interface{}
	Push(_key interface{}, _data interface{})
	Remove(key interface{})
	Head() interface{}
}

var bufMgr *BufferPoolMgr

type BufferPoolMgr struct {
	poolSize    int64 // number of blocks
	diskManager *dsk.DiskMgr
	block       Pool
	freeList    Pool
}

const (
	ALIGN = BLKSIZE
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

func getBufMgr() *BufferPoolMgr {
	return bufMgr
}

func NewBufferPoolMgr(psize int64, fName string) (*BufferPoolMgr, error) {
	if bufMgr != nil {
		return bufMgr, nil
	}
	mgr, err := dsk.NewDiskMgr(fName)
	if err != nil {
		return nil, fmt.Errorf("NewBufferPoolMgr: Unable to create new manager %v", err)
	}
	bufMgr = &BufferPoolMgr{
		poolSize:    alignBlock(mgr.Size())/BLKSIZE,
		diskManager: mgr,
		block:       ds.NewList(),
		freeList:    ds.NewList(),
	}
	return bufMgr, nil
}

func (buf *BufferPoolMgr) NumBlocks() int64 {
	return buf.poolSize
}

func (buf *BufferPoolMgr) Load() error {
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
		blk, err := NewBlock(fData[:BLKSIZE], blockID)
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

func (buf *BufferPoolMgr) GetBlock(blockId int64) (*Block, error) {
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

func (buf *BufferPoolMgr) GetFree(sz int) *Block {
	blk := buf.block.Head()
	if blk == nil {
		blk = buf.freeList.Head()
	}

	if reflect.ValueOf(blk.(*ds.Value)).IsNil() {
		data := make([]byte, 0)
		fileSize := buf.diskManager.Size()
		if buf.NumBlocks() <= 0 {
			_blk, err := NewBlock(data, int(fileSize))
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

func (buf *BufferPoolMgr) flushBlock(blockID int, blk *Block) error {
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

func (buf *BufferPoolMgr) FlushBlock(blockID int) {
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
