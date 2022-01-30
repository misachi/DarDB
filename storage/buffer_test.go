package storage

import (
	"os"
	"path"
	"testing"
)

func getFile(t *testing.T) string {
	dir := t.TempDir()
	var filePath = path.Join(dir, "FooTable")
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Errorf("Open error: %v", err)
	}
	file.Write(sourceData)
	return file.Name()
}

func TestNewBufferPoolMgr(t *testing.T) {
	file := getFile(t)

	poolSize := 5
	pmgr, err := NewBufferPoolMgr(poolSize, file)
	if err != nil {
		t.Errorf("error creating buffer: %v", err)
	}

	if pmgr.poolSize != poolSize {
		t.Errorf("Expected pool size to be %d but got %d", poolSize, pmgr.poolSize)
	}
}

func TestGetBlock(t *testing.T) {
	file := getFile(t)
	poolSize := 5
	blockId := 3
	pmgr, _ := NewBufferPoolMgr(poolSize, file)
	err := pmgr.Load()
	if err != nil {
		t.Errorf("Load error: %v", err)
	}
	blk, err := pmgr.GetBlock(blockId)
	if err != nil {
		t.Errorf("GetBlock error: %v", err)
	}
	if blk.blockId != blockId {
		t.Errorf("GetBlock error: expected block id to be %d but got %d", blockId, blk.blockId)
	}
	if blk.size != BLKSIZE {
		t.Errorf("GetBlock error: expected size to be %d but got %d", BLKSIZE, blk.size)
	}
}
