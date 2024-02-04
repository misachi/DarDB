package db

import (
	"fmt"
	"os"
	"path"
	"testing"

	st "github.com/misachi/DarDB/storage"
)

func getFile(t *testing.T, tblID st.Tbl_t) string {
	dir := t.TempDir()
	var filePath = path.Join(dir, fmt.Sprintf("%d", tblID))
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Errorf("Open error: %v", err)
	}
	file.Write(st.SourceData)
	file.Close()
	return file.Name()
}

func TestNewBufferPoolMgr(t *testing.T) {
	poolSize := 0
	BufMgr = nil
	pmgr, err := NewBufferPoolMgr()
	if err != nil {
		t.Errorf("error creating buffer: %v", err)
	}

	if pmgr.blkCount.Load() != int64(poolSize) {
		t.Errorf("Expected pool size to be %d but got %d", poolSize, pmgr.blkCount.Load())
	}
}

func TestGetBlock(t *testing.T) {
	var blockId st.Blk_t = 3
	var tblId st.Tbl_t = 4
	f := getFile(t, tblId)
	pmgr, _ := NewBufferPoolMgr()
	err := pmgr.Load(tblId, f)
	if err != nil {
		t.Errorf("Load error: %v", err)
	}
	blk, err := pmgr.GetBlock(f, tblId, blockId)
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
