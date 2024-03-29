package db

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	col "github.com/misachi/DarDB/column"
	"github.com/misachi/DarDB/config"
	st "github.com/misachi/DarDB/storage"
	row "github.com/misachi/DarDB/storage/db/row"
)

var _Catalog *Catalog

const CATALOG_PATH = "/tmp/.meta/catalog"

type Catalog struct {
	maxTblID    atomic.Uint64
	maxDbID     atomic.Uint64
	maxTxnID    atomic.Uint64
	maxCommitID atomic.Uint64
	db          map[string]*DB
	mut         *sync.Mutex
}

func load(filePath string, catalog *Catalog) error {
	dbDirs, err := os.ReadDir(filePath)
	if err != nil {
		return fmt.Errorf("load: unable to read directory %s: %v", filePath, err)
	}
	for _, dir := range dbDirs {
		// path :=
		dir.Name()
	}
	return nil
}

func startCatalog(cfg *config.Config, catalog *Catalog) {
	// cfg := config.NewConfig(".old", 0, 0)
	_db := NewDB("catalog", cfg)
	schema := map[string]col.SUPPORTED_TYPE{
		"id":    col.INT64,
		"maxID": col.UINT64,
		"name":  col.STRING,
	}
	pkey := col.NewColumn("id", col.INT64)
	tbl, err := _db.CreateTable("table1", schema, pkey)
	if err != nil {
		slog.Error("startCatalog: %v", err)
		panic(err)
	}

	colData := row.NewColumnData_(
		[]col.Column{col.NewColumn("id", col.INT64), col.NewColumn("maxID", col.UINT64), col.NewColumn("name", col.STRING)},
	)

	ctx := GetClientContextMgr().NewClientCtx(cfg, _db)
	recs, _ := tbl.GetRecord(ctx, "name", []byte("dbID"))
	if len(recs) <= 0 {
		tbl.AddRecord(ctx, colData.Keys(), [][]byte{[]byte("1"), []byte("1"), []byte("dbID")})
		tbl.AddRecord(ctx, colData.Keys(), [][]byte{[]byte("2"), []byte("1"), []byte("tblID")})
		tbl.AddRecord(ctx, colData.Keys(), [][]byte{[]byte("3"), []byte("1"), []byte("txnID")})
		tbl.AddRecord(ctx, colData.Keys(), [][]byte{[]byte("4"), []byte("1"), []byte("commitID")})
		tbl.Flush() // Persist to disk
	}
	recs, _ = tbl.GetRecord(ctx, "name", []byte("dbID"))
	dbID := recs[0].(*row.VarLengthRecord).GetField(colData, "maxID")
	dbIDConv, errDB := strconv.ParseUint(*(*string)(unsafe.Pointer(&dbID)), 10, 64)

	if errDB != nil {
		slog.Error("startCatalog: get max DB ID: %v", errDB)
		panic(errDB)
	}

	if _db.dbID < 1 {
		_db.dbID = st.DB_t(dbIDConv) + 1
	}

	catalog.maxDbID.Store(uint64(dbIDConv))

	recs, err = tbl.GetRecord(ctx, "name", []byte("tblID"))
	if err != nil {
		slog.Error("startCatalog: Get table record: %v", err)
	}
	tblID := recs[0].GetField(colData, "maxID")
	tblIDConv, errTbl := strconv.ParseUint(*(*string)(unsafe.Pointer(&tblID)), 10, 64)
	if errTbl != nil {
		slog.Error("startCatalog: get max table ID: %v", errTbl)
		panic(errTbl)
	}
	catalog.maxTblID.Store(uint64(tblIDConv))

	recs, _ = tbl.GetRecord(ctx, "name", []byte("txnID"))
	txnID := recs[0].GetField(colData, "maxID")
	txnIDConv, errTxn := strconv.ParseUint(*(*string)(unsafe.Pointer(&txnID)), 10, 64)
	if errTxn != nil {
		slog.Error("startCatalog: get max transaction ID: %v", errTxn)
		panic(errTxn)
	}
	catalog.maxTxnID.Store(uint64(txnIDConv))

	recs, _ = tbl.GetRecord(ctx, "name", []byte("commitID"))
	commitID := recs[0].GetField(colData, "maxID")
	commitIDConv, errCommitID := strconv.ParseUint(*(*string)(unsafe.Pointer(&commitID)), 10, 64)
	if errCommitID != nil {
		slog.Error("startCatalog: get max commit ID: %v", errCommitID)
		panic(errCommitID)
	}
	catalog.maxCommitID.Store(uint64(commitIDConv))

	// _db.mut.Lock()
	_db.table[tbl.info.Name] = tbl
	// _db.mut.Unlock()

	if _Catalog == nil {
		_Catalog = catalog
	}

	// catalog.mut.Lock()
	catalog.db = make(map[string]*DB)
	catalog.db[_db.name] = _db
	// catalog.mut.Unlock()

	ctx.Close()
}

func NewCatalog(cfg *config.Config) *Catalog {
	if _Catalog != nil {
		return _Catalog
	}
	catalog := &Catalog{mut: &sync.Mutex{}}
	_Catalog = catalog
	startCatalog(cfg, catalog)
	return catalog
}

func GetCatalog(cfg *config.Config) *Catalog {
	if _Catalog == nil {
		return NewCatalog(cfg)
	}
	return _Catalog
}

func (cat *Catalog) SetMaxTblId(tblId st.Tbl_t) error {
	if st.Tbl_t(cat.maxTblID.Load()) > tblId {
		return fmt.Errorf("SetMaxTblId: Cannot set max ID with lower ID number")
	}
	cat.maxTblID.Store(uint64(tblId))
	return nil
}

func (cat *Catalog) SetMaxDbId(dbId st.DB_t) error {
	if st.DB_t(cat.maxDbID.Load()) > dbId {
		return fmt.Errorf("SetMaxDbId: Cannot set max ID with lower ID number")
	}
	cat.maxDbID.Store(uint64(dbId))
	return nil
}

func (cat *Catalog) SetMaxTxnId(txnId st.Txn_t) error {
	if st.Txn_t(cat.maxTxnID.Load()) > txnId {
		return fmt.Errorf("SetMaxTxnId: Cannot set max ID with lower ID number")
	}
	cat.maxTxnID.Store(uint64(txnId))
	return nil
}

func (cat *Catalog) CompareAndSwapMaxTxnId(txnId st.Txn_t) error {
	if st.Txn_t(cat.maxTxnID.Load()) > txnId {
		return fmt.Errorf("SetMaxTxnId: Cannot set max ID with lower ID number")
	}
	cat.maxTxnID.Store(uint64(txnId))
	return nil
}

func (cat *Catalog) SetMaxCommitId(commitId st.Txn_t) error {
	if st.Txn_t(cat.maxCommitID.Load()) > commitId {
		return fmt.Errorf("SetCommitId: Cannot set max ID with lower ID number")
	}
	cat.maxCommitID.Store(uint64(commitId))
	return nil
}

func (cat *Catalog) MaxCommitId() st.Txn_t {
	return st.Txn_t(cat.maxCommitID.Load())
}

func (cat *Catalog) MaxTblId() st.Tbl_t {
	return st.Tbl_t(cat.maxTblID.Load())
}

func (cat *Catalog) MaxDbId() st.DB_t {
	return st.DB_t(cat.maxDbID.Load())
}

func (cat *Catalog) MaxTxnId() st.Txn_t {
	return st.Txn_t(cat.maxTxnID.Load())
}

// func ReadCatalog() *Catalog {

// }
