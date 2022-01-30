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
	tblPath := path.Join(cfg.DataPath(), dbName, tblInfo.Name)
	m, err := st.NewBufferPoolMgr(5, tblPath)
	if err != nil {
		return nil, fmt.Errorf("NewTable: unable to create a new manager\n %v", err)
	}

	return &Table{
		mgr:  m,
		info: tblInfo,
	}, nil
}

func (tbl *Table) AddRecord(colName string, fieldVal []byte) bool {
	
}

func NewTableInfo(name string, location string, cols []column.Column, pkey column.Column) *TableInfo {
	return &TableInfo{
		Column:   cols,
		Name:     name,
		Location: location,
		Pkey:     pkey,
	}
}

func DSerialize(td *TableInfo) error {
	dsk := st.NewDiskMgr(td.Path)
	data := make([]byte, dsk.Size())
	_, err := dsk.Read(data)
	if err != nil {
		return fmt.Errorf("dSerialize: read error %v", err)
	}

	if err = json.Unmarshal(data, td); err != nil {
		return fmt.Errorf("dSerialize table metadata: Unmarshal error %v", err)
	}
	return nil
}
