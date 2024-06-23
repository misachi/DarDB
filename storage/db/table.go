package db

import (
	"fmt"
	"os"
	"path"

	"github.com/misachi/DarDB/column"
	"github.com/misachi/DarDB/config"
	st "github.com/misachi/DarDB/storage"

	// blk "github.com/misachi/DarDB/storage/database/block"
	row "github.com/misachi/DarDB/storage/db/row"
)

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
	tblID st.Tbl_t
	// internalBuf *BufferPoolMgr
	info *TableInfo
}

func openRWCreate(file string) (*os.File, error) {
	return os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0750)
}

func NewTable(dbName string, tblInfo *TableInfo, cfg *config.Config) (*Table, error) {
	tblPath := path.Join(cfg.DataPath(), dbName, fmt.Sprintf("%s.data", tblInfo.Name))
	// tblID := dbName // & 0xffffffff
	// tblID += 1
	// m, err := NewBufferPoolMgr(0, tblPath, st.Tbl_t(tblID))
	// if err != nil {
	// 	return nil, fmt.Errorf("NewTable: unable to create a new manager\n %v", err)
	// }
	var tblID st.Tbl_t
	catalog := GetCatalog(cfg)
	if catalog != nil {
		if _, ok := catalog.db["catalog"]; ok {
			// newTblID := catalog.maxTblID.Add(1)
			// catalog.SetMaxTblId(st.Tbl_t(newTblID))
			// tblID = catalog.MaxTblId()

			successful := false
			for !successful {
				oldTblID := catalog.MaxTblId()
				tblID = oldTblID + 1
				successful = catalog.maxTblID.CompareAndSwap(uint64(oldTblID), uint64(tblID))
			}
		}
	}

	tblInfo.Location = tblPath

	// tID := fmt.Sprintf("%d", tblID)
	// infoDir := path.Join(cfg.DataPath(), dbName)
	dataDir := path.Join(cfg.DataPath(), dbName)
	// err := os.MkdirAll(infoDir, 0750)
	// if err != nil {
	// 	return nil, fmt.Errorf("CreateTable: MkdirAll infoDir error %v", err)
	// }
	err := os.MkdirAll(dataDir, 0750)
	if err != nil {
		return nil, fmt.Errorf("CreateTable: MkdirAll dataDir error %v", err)
	}

	dataFile, err := openRWCreate(path.Join(dataDir, fmt.Sprintf("%s.data", tblInfo.Name)))
	if err != nil {
		return nil, fmt.Errorf("CreateTable: data file error %v", err)
	}
	defer dataFile.Close()

	// infoFile, err := openRWCreate(path.Join(infoDir, fmt.Sprintf("%s.data", tblInfo.Name)))
	// if err != nil {
	// 	return nil, fmt.Errorf("CreateTable: meta file error %v", err)
	// }
	// defer infoFile.Close()

	return &Table{
		// internalBuf: m,
		info:  tblInfo,
		tblID: tblID,
	}, nil
}

func (tbl *Table) GetInfo() *TableInfo {
	return tbl.info
}

func (tbl *Table) Flush() {
	bufMgr := GetBufMgr()
	bufMgr.Flush(tbl.info.Location, tbl.tblID)
	// var i int64 = 0
	// for i < tbl.mgr.NumBlocks() {
	// 	tbl.mgr.FlushBlock(int(i))
	// 	i++
	// }
}

func (tbl *Table) AddRecord(ctx *ClientContext, cols []column.Column, fieldVals [][]byte) (bool, error) {
	recSize := 0
	for i := 0; i < len(fieldVals); i++ {
		recSize += len(fieldVals[i])
	}

	bufMgr := GetBufMgr()
	blk := bufMgr.GetFree(tbl.info.Location, tbl.tblID, recSize)
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
	bufMgr.WriteBlock(tbl.info.Location, tbl.tblID, blk.BlockID())

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
		bufMgr := GetBufMgr()
		blk, err := bufMgr.GetBlock(tbl.info.Location, tbl.tblID, st.Blk_t(f_block))
		if err != nil {
			return nil, fmt.Errorf("GetRecord: GetBlock: %v", err)
		}
		// fmt.Printf("Block: %q\n", blk.records)
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

func NewTableInfo(name string, cols []column.Column, pkey column.Column) *TableInfo {
	return &TableInfo{
		Column:   cols,
		Name:     name,
		// Location: location,
		Pkey:     pkey,
	}
}

// func DSerialize(td *TableInfo) error {
// 	dsk, err := st.NewDiskMgr(td.Path)
// 	if err != nil {
// 		return fmt.Errorf("DSerialize:  st.NewDiskMgr %v", err)
// 	}
// 	data := make([]byte, dsk.Size())
// 	_, err = dsk.Read(data)
// 	if err != nil {
// 		return fmt.Errorf("DSerialize: read error %v", err)
// 	}

// 	if err = json.Unmarshal(data, td); err != nil {
// 		return fmt.Errorf("DSerialize table metadata: Unmarshal error %v", err)
// 	}
// 	return nil
// }
