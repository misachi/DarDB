package db

import (
	"fmt"
	"os"
	"sync"
)

var _Catalog *Catalog
var catMut *sync.RWMutex

const CATALOG_PATH = "/tmp/.meta/catalog"

type Catalog struct {
	db map[string]*DB
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