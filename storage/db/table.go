package db

import (
	"encoding/json"
	"fmt"
	"path"

	"github.com/misachi/DarDB/column"
	"github.com/misachi/DarDB/config"
	st "github.com/misachi/DarDB/storage"

	// blk "github.com/misachi/DarDB/storage/database/block"
	row "github.com/misachi/DarDB/storage/db/row"
)

type tbl_t uint64

// const BLKSIZE = 4096 // Size of block on disk

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
	tblID       tbl_t
	internalBuf *BufferPoolMgr
	info        *TableInfo
}

func NewTable(dbName db_t, tblInfo *TableInfo, cfg *config.Config) (*Table, error) {
	tblPath := path.Join(cfg.DataPath(), fmt.Sprintf("%d", dbName), tblInfo.Name, fmt.Sprintf("%s.data", tblInfo.Name))
	tblID := dbName & 0xffffffff
	tblID += 1
	m, err := NewBufferPoolMgr(0, tblPath, tbl_t(tblID))
	if err != nil {
		return nil, fmt.Errorf("NewTable: unable to create a new manager\n %v", err)
	}

	return &Table{
		internalBuf: m,
		info:        tblInfo,
	}, nil
}

func (tbl *Table) GetInfo() *TableInfo {
	return tbl.info
}

func (tbl *Table) Flush() {
	tbl.internalBuf.FlushBlock(0)
	// var i int64 = 0
	// for i < tbl.mgr.NumBlocks() {
	// 	tbl.mgr.FlushBlock(int(i))
	// 	i++
	// }
}

func (tbl *Table) AddRecord(cols []column.Column, fieldVals [][]byte) (bool, error) {
	recSize := 0
	for i := 0; i < len(fieldVals); i++ {
		recSize += len(fieldVals[i])
	}

	blk := tbl.internalBuf.GetFree(recSize)
	if blk == nil {
		return false, fmt.Errorf("AddRecord: check disk space")
	}

	record, err := row.NewVarLengthRecord(cols, fieldVals)
	if err != nil {
		return false, fmt.Errorf("AddRecord: record error %v", err)
	}

	if err := blk.AddRecord(record); err != nil {
		return false, fmt.Errorf("AddRecord: %v", err)
	}

	return true, nil
}

func (tbl *Table) GetRecord(ctx *ClientContext, colName string, colValue []byte) ([]row.Record, error) {
	var f_block int64 = 0
	records := make([]row.Record, 0)
	var tblSz int64 = 1 //tbl.info.NumBlocks

	for {
		if tblSz < f_block {
			break
		}
		blk, err := tbl.internalBuf.GetBlock(f_block)
		if err != nil {
			return nil, fmt.Errorf("GetRecord: GetBlock: %v", err)
		}
		rec, err := blk.FilterRecords(ctx, row.NewColumnData_(tbl.info.Column), colName, colValue)
		// txn := ctx.CurrentTxn()
		// txn.TxnReadRecords(rec)
		if err != nil {
			return nil, fmt.Errorf("GetRecord: FilterRecords: %v", err)
		}
		records = append(records, rec...)
		f_block += BLKSIZE
	}
	return records, nil
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
