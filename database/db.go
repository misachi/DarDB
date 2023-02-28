package database

import (
	"fmt"
	"os"
	"path"

	"github.com/misachi/DarDB/column"
	"github.com/misachi/DarDB/config"

	// st "github.com/misachi/DarDB/storage"
	tbl "github.com/misachi/DarDB/table"
)

type DB struct {
	name   string
	table  map[string]*tbl.Table
	config *config.Config
	// tableData map[string]*tbl.TableInfo
}

func NewDB(dbName string, cfg *config.Config) *DB {
	dbPath := path.Join(cfg.DataPath(), dbName)
	err := os.MkdirAll(dbPath, 0750)
	if err != nil {
		return nil
	}
	return &DB{
		name:   dbName,
		config: cfg,
		table:  make(map[string]*tbl.Table),
	}
}

func openRWCreate(file string) (*os.File, error) {
	return os.OpenFile(file, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0750)
}

func (db *DB) CreateTable(tblName string, cols map[string]column.SUPPORTED_TYPE, pkey column.Column) (*tbl.Table, error) {
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
	infoDir := path.Join(db.config.DataPath(), db.name, tblName)
	dataDir := path.Join(db.config.DataPath(), db.name, tblName, ".meta")
	err := os.MkdirAll(infoDir, 0750)
	if err != nil {
		return nil, fmt.Errorf("CreateTable: MkdirAll infoDir error %v", err)
	}
	err = os.MkdirAll(dataDir, 0750)
	if err != nil {
		return nil, fmt.Errorf("CreateTable: MkdirAll dataDir error %v", err)
	}

	dataFile, err := openRWCreate(path.Join(dataDir, fmt.Sprintf("%s.data", tblName)))
	if err != nil {
		return nil, fmt.Errorf("CreateTable: data file error %v", err)
	}
	defer dataFile.Close()

	infoFile, err := openRWCreate(path.Join(infoDir, fmt.Sprintf("%s.data", tblName)))
	if err != nil {
		return nil, fmt.Errorf("CreateTable: meta file error %v", err)
	}
	defer infoFile.Close()

	tblInfo := tbl.NewTableInfo(tblName, infoFile.Name(), schema, pkey)

	tb, err := tbl.NewTable(db.name, tblInfo, db.config)
	if err != nil {
		return nil, fmt.Errorf("CreateTable: NewTable error %v", err)
	}
	db.table[tblName] = tb
	return tb, nil
}

func (db *DB) AddRecord(tbl *tbl.Table, data map[string][]byte) error {
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
		return fmt.Errorf("DB AddRecord: %v\n", err)
	}
	return nil
}

// func (db *DB) GetRecord(fieldKey string, fieldVal []byte) st.VarLengthRecord {

// }
