package db

import (
	"bytes"
	"testing"

	"github.com/misachi/DarDB/column"
	"github.com/misachi/DarDB/config"
	"github.com/misachi/DarDB/storage"
	"github.com/misachi/DarDB/storage/db/row"
)

func TestNewDB(t *testing.T) {
	_Catalog = nil
	cfg := config.NewConfig(t.TempDir(), 1, 1)
	db := NewDB("testDB", cfg)

	if db.dbID != 2 {
		t.Errorf("TestNewDB: Expected ID %d but found %d", 2, db.dbID)
	}

	if db.name != "testDB" {
		t.Errorf("TestNewDB: Expected name `%s` but found `%s`", "testDB", db.name)
	}
}

func TestCreateTable(t *testing.T) {
	_Catalog = nil
	cfg := config.NewConfig(t.TempDir(), 1, 1)
	db := NewDB("testDB", cfg)

	pkey := column.Column{Name: "id1", Type: column.INT}

	type valType struct {
		givenTableName string
		givenCols      map[string]column.SUPPORTED_TYPE
		wantTableName  string
		wantTableID    int64
	}

	values := []valType{
		{
			givenTableName: "table101",
			givenCols: map[string]column.SUPPORTED_TYPE{
				"id1": column.INT,
				"id2": column.INT,
				"id3": column.INT,
				"id4": column.INT,
			},
			wantTableName: "table101",
			wantTableID:   2,
		},
		{
			givenTableName: "table103",
			givenCols: map[string]column.SUPPORTED_TYPE{
				"id1": column.INT,
				"id2": column.INT,
			},
			wantTableName: "table103",
			wantTableID:   3,
		},
	}

	for _, val := range values {
		table, err := db.CreateTable(val.givenTableName, val.givenCols, pkey)
		if err != nil {
			t.Errorf("TestCreateTable: %v", err)
		}

		if table.tblID != storage.Tbl_t(val.wantTableID) {
			t.Errorf("TestCreateTable: Expected ID %d but found %d", val.wantTableID, table.tblID)
		}

		if table.info.Name != val.wantTableName {
			t.Errorf("TestCreateTable: Expected name `%s` but found `%s`", val.wantTableName, table.info.Name)
		}
	}
}

func TestGetRecord(t *testing.T) {
	type valType struct {
		givenData      []map[string][]byte
		givenSearchKey []byte
		givenColName   string
		wantRecord     []byte
		wantRecordLen  int
	}

	values := []valType{
		{
			givenData: []map[string][]byte{
				{
					"id1": []byte("2"),
					"id2": []byte("10"),
				},
				{
					"id1": []byte("6"),
					"id2": []byte("15"),
				},
			},
			givenColName:   "id1",
			givenSearchKey: []byte("2"),
			wantRecord:     []byte("3\n0,1:2,2\n2:10"),
			wantRecordLen:  1,
		},
		{
			givenData: []map[string][]byte{
				{
					"id1": []byte("2"),
					"id2": []byte("10"),
				},
				{
					"id1": []byte("6"),
					"id2": []byte("15"),
				},
			},
			givenColName:   "id1",
			givenSearchKey: []byte("6"),
			wantRecord:     []byte("3\n0,1:2,2\n6:15"),
			wantRecordLen:  1,
		},
	}

	for _, val := range values {
		_Catalog = nil
		cfg := config.NewConfig(t.TempDir(), 1, 1)
		db := NewDB("testDB", cfg)
		ctx := GetClientContextMgr().NewClientCtx(cfg, db)

		cols := map[string]column.SUPPORTED_TYPE{
			"id1": column.INT,
			"id2": column.INT,
		}
		pkey := column.Column{Name: "id1", Type: column.INT}
		table, err := db.CreateTable("table101", cols, pkey)
		if err != nil {
			t.Errorf("TestGetRecord: %v", err)
		}

		for _, data := range val.givenData {
			err := db.AddRecord(ctx, table, data)
			if err != nil {
				t.Errorf("TestGetRecord: Error adding new record: %v", err)
			}
		}

		rec, err := db.GetRecord(ctx, table, val.givenColName, val.givenSearchKey)
		if err != nil {
			t.Errorf("TestGetRecord: Error getting record: %v", err)
		}

		if len(rec) != val.wantRecordLen {
			t.Errorf("TestGetRecord: Expected length to be %d but found %d", val.wantRecordLen, len(rec))
		}

		varLenRecord := rec[0].(*row.VarLengthRecord)
		if !bytes.Equal(varLenRecord.ToByte(), val.wantRecord) {
			t.Errorf("TestGetRecord: Expected row %q but found %q", val.wantRecord, varLenRecord.ToByte())
		}
	}
}
