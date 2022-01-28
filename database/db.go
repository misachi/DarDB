package database

import (
	"bytes"
	"fmt"
	"os"

	"github.com/misachi/DarDB/column"
	tbl "github.com/misachi/DarDB/table"
)

const (
	DATA_DIR = "/tmp/%s/%s"
	META_DIR = "/tmp/%s/%s/.meta/"
)

type DB struct {
	name  string
	table map[string]*tbl.Table
	// tableData map[string]*tbl.TableInfo
}

func NewDB(dbName string) *DB {
	return &DB{
		name: dbName,
	}
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
	infoLocation := fmt.Sprintf(META_DIR, db.name, tblName)
	dataLocation := fmt.Sprintf(DATA_DIR, db.name, tblName)
	tblInfo := tbl.NewTableInfo(tblName, infoLocation, schema, pkey)

	tblData, err := os.ReadFile(dataLocation)
	if err != nil {
		return nil, fmt.Errorf("CreateTable: ReadFile error")
	}
	r := bytes.NewReader(tblData)
	tb, err := tbl.NewTable(r, tblInfo)
	if err != nil {
		return nil, fmt.Errorf("CreateTable: NewTable error %v", err)
	}
	db.table[tblName] = tb
	return tb, nil
}

func (db *DB) AddRecord(tbl *tbl.Table, data map[string][]byte) bool {
	return false
}

