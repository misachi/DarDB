package db

import (
	"os"
	"path"
	"testing"

	st "github.com/misachi/DarDB/storage"
)

func getFile(t *testing.T) string {
	dir := t.TempDir()
	var filePath = path.Join(dir, "FooTable")
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Errorf("Open error: %v", err)
	}
	file.Write(st.SourceData)
	return file.Name()
}

func TestNewBufferPoolMgr(t *testing.T) {
	file := getFile(t)

	poolSize := 3
	pmgr, err := NewBufferPoolMgr(int64(poolSize), file)
	if err != nil {
		t.Errorf("error creating buffer: %v", err)
	}

	if pmgr.poolSize != int64(poolSize) {
		t.Errorf("Expected pool size to be %d but got %d", poolSize, pmgr.poolSize)
	}
}

func TestGetBlock(t *testing.T) {
	file := getFile(t)
	poolSize := 5
	blockId := 3
	pmgr, _ := NewBufferPoolMgr(int64(poolSize), file)
	err := pmgr.Load()
	if err != nil {
		t.Errorf("Load error: %v", err)
	}
	blk, err := pmgr.GetBlock(int64(blockId))
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
