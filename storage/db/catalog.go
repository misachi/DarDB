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

func NewCatalog() *Catalog {
	if _Catalog != nil {
		return _Catalog
	}
	return &Catalog{}
}

func GetCatalog() *Catalog {
	catMut.RLock()
	defer catMut.RUnlock()
	if _Catalog == nil {
		return nil
	}
	return _Catalog
}