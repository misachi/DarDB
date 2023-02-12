package storage

import (
	"fmt"
	"reflect"

	ds "github.com/misachi/DarDB/structure"
)

type Pool interface {
	Pop() interface{}
	Get(key interface{}) interface{}
	Push(_key interface{}, _data interface{})
	Remove(key interface{})
	Head() interface{}
}

type BufferPoolMgr struct {
	poolSize    int
	diskManager *DiskMgr
	block       Pool
	freeList    Pool
}

func NewBufferPoolMgr(psize int, fName string) (*BufferPoolMgr, error) {
	mgr, err := NewDiskMgr(fName)
	if err != nil {
		return nil, fmt.Errorf("NewBufferPoolMgr: Unable to create new manager %v\n", err)
	}
	return &BufferPoolMgr{
		poolSize:    psize,
		diskManager: mgr,
		block:       ds.NewList(),
		freeList:    ds.NewList(),
	}, nil
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
		if err != nil {
			return fmt.Errorf("Load: error creating new block %v", err)
		}
		buf.block.Push(blk.blockId, blk)
		fData = fData[BLKSIZE:]
		blockID += 1
	}
	return nil
}

func (buf *BufferPoolMgr) GetBlock(blockId int) (*Block, error) {
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
	blk, err := NewBlock(blkData, int(BLKSIZE/blockId))
	if err != nil {
		return nil, fmt.Errorf("GetBlock: new block error %v", err)
	}
	return blk, nil
}

func (buf *BufferPoolMgr) GetFree(sz int) *Block {
	blk := buf.block.Head()
	if blk == nil {
		blk = buf.freeList.Head()
	}

	// We don't have a free block in the freeList
	if reflect.ValueOf(blk.(*ds.Value)).IsNil() {
		data := make([]byte, 0)
		fileSize := buf.diskManager.Size()
		_blk, err := NewBlock(data, int(fileSize))
		if err != nil {
			return nil
		}
		blk = _blk
		buf.block.Push(fileSize, _blk)
	}
	if blk != nil && blk.(*Block).size < sz {
		return blk.(*Block)
	}
	return nil
}

func (buf *BufferPoolMgr) flushBlock(blockID int, blk *Block) {
	buf.diskManager.Seek(int64(blockID), 0)
	buf.diskManager.Write(blk.ToByte())
	buf.freeList.Push(blockID, blk)
	buf.block.Remove(blockID)
}

func (buf *BufferPoolMgr) FlushBlock(blockID int) {
	blk := buf.block.Get(blockID)
	if blk != nil {
		buf.flushBlock(blockID, blk.(*Block))
	}
}
