package main

import (
	"fmt"

	col "github.com/misachi/DarDB/column"
	"github.com/misachi/DarDB/config"
	db "github.com/misachi/DarDB/storage/db"
)

func main() {
	cfg := config.NewConfig("/tmp/DarDB", 0, 0)
	_db := db.NewDB("myDB", cfg)
	ctx := db.GetClientContextMgr().NewClientCtx(cfg, _db)
	schema := map[string]col.SUPPORTED_TYPE{
		"id":   col.INT64,
		"name": col.STRING,
	}
	pkey := col.NewColumn("id", col.INT64)
	tbl, err := _db.CreateTable("table1", schema, pkey)
	if err != nil {
		fmt.Println(err)
	}

	data := map[string][]byte{
		"id":   []byte("123"),
		"name": []byte("HeIsYOu"),
	}
	_db.AddRecord(ctx, tbl, data)
	tbl.Flush()

	recs, err := _db.GetRecord(ctx, tbl, "name", []byte("HeIsYOu"))
	if err != nil {
		fmt.Printf("Recs result: %s\n", err)
	}
	fmt.Printf("Got it: %q\n", recs[0])
}
