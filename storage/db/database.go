package db

import (
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/misachi/DarDB/column"
	cfg "github.com/misachi/DarDB/config"

	row "github.com/misachi/DarDB/storage/db/row"
	st "github.com/misachi/DarDB/storage"
)

// type db_t uint64

type DB struct {
	dbID   st.DB_t
	name   string
	table  map[string]*Table
	mut    *sync.RWMutex
	config *cfg.Config
}

func NewDB(dbName string, cfg *cfg.Config) *DB {
	var dbID st.DB_t
	dbPath := path.Join(cfg.DataPath(), dbName)
	err := os.MkdirAll(dbPath, 0750)
	if err != nil {
		return nil
	}

	catalog := _Catalog
	if catalog != nil {
		if  _, ok := catalog.db["catalog"]; ok {
			newDBID := catalog.maxDbID.Add(1)
			catalog.SetMaxDbId(st.DB_t(newDBID))
			dbID = catalog.MaxDbId()
		}
	}

	return &DB{
		name:   dbName,
		config: cfg,
		table:  make(map[string]*Table),
		dbID:   dbID,
	}
}

func (db *DB) CreateTable(tblName string, cols map[string]column.SUPPORTED_TYPE, pkey column.Column) (*Table, error) {
	if _, ok := db.table[tblName]; ok {
		return nil, fmt.Errorf("CreateTable: Table already exists")
	}
	schema := make([]column.Column, 0)

	varLenKeys := make([]column.Column, 0)
	for name, _type := range cols {
		if _type == column.STRING {
			varLenKeys = append(varLenKeys, column.NewColumn(name, _type))
		} else {
			schema = append(schema, column.NewColumn(name, _type))
		}
	}
	schema = append(schema, varLenKeys...)

	tblInfo := NewTableInfo(tblName, schema, pkey)

	tb, err := NewTable(db.name, tblInfo, db.config)
	if err != nil {
		return nil, fmt.Errorf("CreateTable: NewTable error %v", err)
	}
	db.mut.Lock()
	db.table[tblName] = tb
	db.mut.Unlock()
	return tb, nil
}

func (db *DB) GetTable(tblName string) *Table {
	if table, ok := db.table[tblName]; ok {
		return table
	}
	return nil
}

func (db *DB) AddRecord(ctx *ClientContext, tbl *Table, data map[string][]byte) error {
	fields := make([]column.Column, 0)
	fieldVals := make([][]byte, 0)
	columns := tbl.GetInfo().Column
	for field, val := range data {
		for _, col := range columns {
			if col.Name == field {
				fields = append(fields, col)
				fieldVals = append(fieldVals, val)
				break
			}
		}
	}
	_, err := tbl.AddRecord(fields, fieldVals)
	if err != nil {
		return fmt.Errorf("DB AddRecord: %v", err)
	}
	return nil
}

func (db *DB) GetRecord(ctx *ClientContext, tbl *Table, colName string, colVal []byte) ([]row.Record, error) {
	records, err := tbl.GetRecord(ctx, colName, colVal)
	if err != nil {
		return nil, fmt.Errorf("GetRecord: Unable to retrieve table records: %v", err)
	}
	return records, nil
}

func (db *DB) Flush(tblName string) {
	db.table[tblName].Flush()
}

// func (db *DB) GetRecord(fieldKey string, fieldVal []byte) st.VarLengthRecord {

// }
