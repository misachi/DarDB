package storage

import (
	"encoding/json"
	"fmt"
	"path"

	"github.com/misachi/DarDB/column"
	"github.com/misachi/DarDB/config"
	st "github.com/misachi/DarDB/storage"
)

var TableMgr map[string]*st.BufferPoolMgr

/* TableInfo represents table meta data */
type TableInfo struct {
	NumBlocks  int             `json:"num_blocks,omitempty"`
	NumRecords int64           `json:"num_records,omitempty"`
	Name       string          `json:"name,omitempty"`
	Location   string          `json:"location,omitempty"`
	Path       string          `json:"path,omitempty"`
	Pkey       column.Column   `json:"pkey,omitempty"`
	Column     []column.Column `json:"schema,omitempty"`
}

type Table struct {
	mgr  *st.BufferPoolMgr
	info *TableInfo
}

func NewTable(dbName string, tblInfo *TableInfo, cfg *config.Config) (*Table, error) {
	tblPath := path.Join(cfg.DataPath(), dbName, tblInfo.Name, fmt.Sprintf("%s.data", tblInfo.Name))
	m, err := st.NewBufferPoolMgr(5, tblPath)
	if err != nil {
		return nil, fmt.Errorf("NewTable: unable to create a new manager\n %v", err)
	}

	return &Table{
		mgr:  m,
		info: tblInfo,
	}, nil
}

func (tbl *Table) GetInfo() *TableInfo {
	return tbl.info
}

func (tbl *Table) Flush() {
	tbl.mgr.FlushBlock(0)
}

func (tbl *Table) AddRecord(cols []column.Column, fieldVals [][]byte) (bool, error) {
	recSize := 0
	for i := 0; i < len(fieldVals); i++ {
		recSize += len(fieldVals[i])
	}
	blk := tbl.mgr.GetFree(recSize)
	if blk == nil {
		return false, fmt.Errorf("AddRecord: check disk space")
	}
	record, err := st.NewVarLengthRecord(cols, fieldVals)
	if err != nil {
		return false, fmt.Errorf("AddRecord: record error %v", err)
	}
	if err := blk.AddRecord(record.ToByte()); err != nil {
		return false, fmt.Errorf("AddRecord: %v", err)
	}
	return true, nil
}

func (tbl *Table) GetRecord() {}

func NewTableInfo(name string, location string, cols []column.Column, pkey column.Column) *TableInfo {
	return &TableInfo{
		Column:   cols,
		Name:     name,
		Location: location,
		Pkey:     pkey,
	}
}

func DSerialize(td *TableInfo) error {
	dsk, err := st.NewDiskMgr(td.Path)
	if err != nil {
		return fmt.Errorf("DSerialize:  st.NewDiskMgr %v", err)
	}
	data := make([]byte, dsk.Size())
	_, err = dsk.Read(data)
	if err != nil {
		return fmt.Errorf("DSerialize: read error %v", err)
	}

	if err = json.Unmarshal(data, td); err != nil {
		return fmt.Errorf("DSerialize table metadata: Unmarshal error %v", err)
	}
	return nil
}
