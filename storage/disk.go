package storage

import (
	"fmt"
	"os"
)

type DiskMgr struct {
	file *os.File
	size int64
}

func NewDiskMgr(fName string) (*DiskMgr, error) {
	f, err := os.OpenFile(fName, os.O_RDWR, 0750)
	if err != nil {
		return nil, fmt.Errorf("NewDiskMgr: os.OpenFile error %v", err)
	}
	fInfo, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("NewDiskMgr: Stat error %v", err)
	}
	return &DiskMgr{file: f, size: fInfo.Size()}, nil
}

func (d *DiskMgr) Size() int64 {
	return d.size
}

func (d *DiskMgr) Read(p []byte) (n int, err error) {
	return d.file.Read(p)
}

func (d *DiskMgr) Seek(offset int64, whence int) (int64, error) {
	return d.file.Seek(offset, whence)
}

func (d *DiskMgr) Write(p []byte) (n int, err error) {
	fInfo, _ := d.file.Stat()
	fSize := fInfo.Size()
	if fSize <= 0 {
		return d.file.Write(p)
	}
	return d.file.WriteAt(p, fSize)
}

func (d *DiskMgr) Flush() error {
	return d.file.Sync()
}
