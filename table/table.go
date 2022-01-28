package storage

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/misachi/DarDB/column"
	st "github.com/misachi/DarDB/storage"
)

var TableMgr map[string]*st.BlockMgr

/* TableInfo represents table meta data */
type TableInfo struct {
	NumBlocks  int             `json:"num_blocks,omitempty"`
	NumRecords int64           `json:"num_records,omitempty"`
	Name       string          `json:"name,omitempty"`
	Location   string          `json:"location,omitempty"`
	Pkey       column.Column   `json:"pkey,omitempty"`
	Column     []column.Column `json:"schema,omitempty"`
}

type Table struct {
	mgr  *st.BlockMgr
	info *TableInfo
}

func NewTable(r io.Reader, tblInfo *TableInfo) (*Table, error) {
	// data, err := os.ReadFile(tblName)
	// if err != nil {
	// 	return nil, fmt.Errorf("NewTable: os.ReadFile\n %v", err)
	// }
	m, err := st.NewBlockMgr(r, -1)
	if err != nil {
		return nil, fmt.Errorf("NewTable: unable to create a new manager\n %v", err)
	}
	return &Table{
		mgr:  m,
		info: tblInfo,
	}, nil
}

func NewTableInfo(name string, location string, cols []column.Column, pkey column.Column) *TableInfo {
	return &TableInfo{
		Column:   cols,
		Name:     name,
		Location: location,
		Pkey:     pkey,
	}
}

func dSerialize(r io.Reader, td *TableInfo) error {
	mgr, err := st.NewBlockMgr(r, -1)
	if err != nil {
		return fmt.Errorf("dSerialize table metadata: unable to create a new manager\n %v", err)
	}
	blockW := mgr.BlockW()
	data := make([]byte, 0)
	for blockW.Block != nil {
		recs, _ := blockW.Block.Records()
		for _, record := range recs {
			field := record.(*st.VarLengthRecord).Field()
			data = append(data, field...)
		}

		blockW = blockW.Next()
	}
	err = json.Unmarshal(data, td)
	if err != nil {
		return fmt.Errorf("dSerialize table metadata: Unmarshal error %v", err)
	}
	return nil
}
