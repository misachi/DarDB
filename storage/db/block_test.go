package db

import (
	"bytes"
	"testing"

	"github.com/misachi/DarDB/column"
	"github.com/misachi/DarDB/config"
	st "github.com/misachi/DarDB/storage"
	row "github.com/misachi/DarDB/storage/db/row"
)

// func TestCreateBlockQ(t *testing.T) {
// 	type valType struct {
// 		given        []byte
// 		wantSize     []int
// 		wantLocation [][]LocationPair
// 	}
// 	values := []valType{
// 		{
// 			given:        []byte("63\n0,20:21,50\n0000000000000000000000000000000000000000000000000"),
// 			wantSize:     []int{63},
// 			wantLocation: [][]LocationPair{{{0, 20}, {21, 50}}},
// 		},
// 		{
// 			given:        sourceData,
// 			wantSize:     []int{4096, 4096, 4096},
// 			wantLocation: [][]LocationPair{{{0, 3}, {4, 50}, {51, 1000}, {1001, 3900}}},
// 		},
// 	}
// 	for _, val := range values {
// 		block, err := createBlockQ(val.given)
// 		if err != nil {
// 			t.Error(err)
// 		}

// 		i := 0
// 		for block != nil {
// 			if block.Block.size != val.wantSize[i] {
// 				t.Errorf("expected block size to be %d but got %d", val.wantSize[i], block.Block.size)
// 			}

// 			for _, locations := range val.wantLocation {
// 				for j, loc := range locations {
// 					if loc.offset != block.Block.recLocation[j].offset || loc.size != block.Block.recLocation[j].size {
// 						t.Errorf("TestNewBlock: Expected locations to be %v but found %v", loc, block.Block.recLocation[j])
// 					}
// 				}
// 			}
// 			block = block.next
// 			i += 1
// 		}
// 	}
// }

func TestNewBlock(t *testing.T) {
	data := []byte("107\n0,58:58,34\n127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasyou127\n0,2:3,2:6,4:11,2\n12:34:1467:56")
	block := Block{
		size:        107,
		recLocation: []BlockLocationPair{{row.NewLocationPair(0, 58), &st.Lock{}}, {row.NewLocationPair(58, 34), &st.Lock{}}},
		records:     []byte("127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasyou127\n0,2:3,2:6,4:11,2\n12:34:1467:56"),
	}
	newBlock, err := NewBlock(data, 1, 0)
	if err != nil {
		t.Error(err)
	}
	if newBlock.size != block.size {
		t.Errorf("TestNewBlock: Expected size to be %d but found %d", block.size, newBlock.size)
	}

	if !bytes.Equal(block.records, newBlock.records) {
		t.Errorf("TestNewBlock: Expected %q bytes in record but got %q", block.records, newBlock.records)
	}

	for i, loc := range block.recLocation {
		if loc.Offset() != newBlock.recLocation[i].Offset() || loc.Size() != newBlock.recLocation[i].Size() {
			t.Errorf("TestNewBlock: Expected locations to be %v but found %v", loc, newBlock.recLocation[i])
		}
	}
}

func TestSetBlockLocation(t *testing.T) {
	type valType struct {
		given        []byte
		wantLocation []BlockLocationPair
	}
	values := []valType{
		{
			given:        []byte("0,20:21,50"),
			wantLocation: []BlockLocationPair{{row.NewLocationPair(0, 20), &st.Lock{}}, {row.NewLocationPair(21, 50), &st.Lock{}}},
		},
		{
			given: []byte("0,3:4,50:51,1000:1001,3900"),
			wantLocation: []BlockLocationPair{
				{row.NewLocationPair(0, 3), &st.Lock{}},
				{row.NewLocationPair(4, 50), &st.Lock{}},
				{row.NewLocationPair(51, 1000), &st.Lock{}},
				{row.NewLocationPair(1001, 3900), &st.Lock{}},
			},
		},
	}

	for i, val := range values {
		location, err := setBlockLocation(val.given)
		if err != nil {
			t.Error(err)
		}
		if location[i].Offset() != val.wantLocation[i].Offset() || location[i].Size() != val.wantLocation[i].Size() {
			t.Errorf("TestSetBlockLocation: Expected locations to be %v but found %v", location, val.wantLocation[i])
		}
	}
}

func TestBlockToByte(t *testing.T) {
	type valType struct {
		given       Block
		wantByteStr []byte
	}

	block := Block{
		size:        123,
		recLocation: []BlockLocationPair{{row.NewLocationPair(0, 20), &st.Lock{}}, {row.NewLocationPair(21, 50), &st.Lock{}}},
		records:     []byte("0000000000000000000000000000000000000000000000000"),
	}
	value := valType{
		given:       block,
		wantByteStr: []byte("123\n0,20:21,50\n0000000000000000000000000000000000000000000000000"),
	}

	byteStr := block.ToByte()
	if !bytes.Equal(byteStr, value.wantByteStr) {
		t.Errorf("TestBlockToByte: Expected byte-string %q but found %q", value.wantByteStr, byteStr)
	}
}

func TestAddRecordWithBytes(t *testing.T) {
	type valType struct {
		data             []byte
		given            Block
		wantRecLocations []BlockLocationPair
		wantRecords      []byte
	}

	values := []valType{
		{
			data: []byte("127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasher"),
			given: Block{
				size:        123,
				recLocation: []BlockLocationPair{{row.NewLocationPair(0, 58), &st.Lock{}}, {row.NewLocationPair(58, 58), &st.Lock{}}},
				records:     []byte("127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasyou127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwashim"),
			},
			wantRecLocations: []BlockLocationPair{{row.NewLocationPair(0, 58), &st.Lock{}}, {row.NewLocationPair(58, 58), &st.Lock{}}, {row.NewLocationPair(116, 58), &st.Lock{}}},
			wantRecords:      []byte("127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasyou127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwashim127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasher"),
		},
		{
			data: []byte("127\n0,2:3,2:6,4:11,2\n12:34:1467:56"),
			given: Block{
				size:        123,
				recLocation: []BlockLocationPair{{row.NewLocationPair(0, 58), &st.Lock{}}, {row.NewLocationPair(58, 58), &st.Lock{}}},
				records:     []byte("127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasyou127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwashim"),
			},
			wantRecLocations: []BlockLocationPair{{row.NewLocationPair(0, 58), &st.Lock{}}, {row.NewLocationPair(58, 58), &st.Lock{}}, {row.NewLocationPair(116, 34), &st.Lock{}}},
			wantRecords:      []byte("127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasyou127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwashim127\n0,2:3,2:6,4:11,2\n12:34:1467:56"),
		},
	}

	for _, value := range values {
		block := value.given
		err := block.AddRecordWithBytes(value.data)
		if err != nil {
			t.Errorf("TestAddRecordWithBytes: %v", err)
		}

		for i, loc := range value.wantRecLocations {
			if loc.Offset() != block.recLocation[i].Offset() || loc.Size() != block.recLocation[i].Size() {
				t.Errorf("TestAddRecordWithBytes: Expected location %v but found %v", loc, block.recLocation[i])
			}
		}
	}

}

func TestAddRecord(t *testing.T) {
	type valType struct {
		data             []byte
		given            Block
		wantRecLocations []BlockLocationPair
		wantRecords      []byte
	}

	values := []valType{
		{
			data: []byte("127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasher"),
			given: Block{
				size:        123,
				recLocation: []BlockLocationPair{{row.NewLocationPair(0, 58), &st.Lock{}}, {row.NewLocationPair(58, 58), &st.Lock{}}},
				records:     []byte("127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasyou127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwashim"),
			},
			wantRecLocations: []BlockLocationPair{{row.NewLocationPair(0, 58), &st.Lock{}}, {row.NewLocationPair(58, 58), &st.Lock{}}, {row.NewLocationPair(116, 58), &st.Lock{}}},
			wantRecords:      []byte("127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasyou127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwashim127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasher"),
		},
		{
			data: []byte("127\n0,2:3,2:6,4:11,2\n12:34:1467:56"),
			given: Block{
				size:        123,
				recLocation: []BlockLocationPair{{row.NewLocationPair(0, 58), &st.Lock{}}, {row.NewLocationPair(58, 58), &st.Lock{}}},
				records:     []byte("127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasyou127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwashim"),
			},
			wantRecLocations: []BlockLocationPair{{row.NewLocationPair(0, 58), &st.Lock{}}, {row.NewLocationPair(58, 58), &st.Lock{}}, {row.NewLocationPair(116, 34), &st.Lock{}}},
			wantRecords:      []byte("127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasyou127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwashim127\n0,2:3,2:6,4:11,2\n12:34:1467:56"),
		},
	}

	for _, value := range values {
		block := value.given
		record, _ := row.NewVarLengthRecordWithHDR(value.data)
		err := block.AddRecord(record)
		if err != nil {
			t.Errorf("TestAddRecordWithBytes: %v", err)
		}

		for i, loc := range value.wantRecLocations {
			if loc.Offset() != block.recLocation[i].Offset() || loc.Size() != block.recLocation[i].Size() {
				t.Errorf("TestAddRecordWithBytes: Expected location %v but found %v", loc, block.recLocation[i])
			}
		}
	}

}

func TestRecords(t *testing.T) {
	type valType struct {
		given              Block
		wantRecordsByteStr [][]byte
	}

	blk := Block{
		size:        107,
		recLocation: []BlockLocationPair{{row.NewLocationPair(0, 58), &st.Lock{}}, {row.NewLocationPair(58, 34), &st.Lock{}}},
		records:     []byte("127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasyou127\n0,2:3,2:6,4:11,2\n12:34:1467:56"),
	}
	block, _ := NewBlock(blk.ToByte(), 0, 1)
	value := valType{
		given: *block,
		wantRecordsByteStr: [][]byte{
			[]byte("127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasyou"),
			[]byte("127\n0,2:3,2:6,4:11,2\n12:34:1467:56"),
		},
	}

	cfg := config.NewConfig(t.TempDir(), 1, 1)
	db := NewDB("test", cfg)
	ctx := GetClientContextMgr().NewClientCtx(cfg, db)
	recs, err := value.given.Records(ctx)
	if err != nil {
		t.Errorf("TestRecords: %v", err)
	}

	for i, rec := range recs {
		if !bytes.Equal(rec.(*row.VarLengthRecord).ToByte(), value.wantRecordsByteStr[i]) {
			t.Errorf("TestRecords: Expected byte-string %v but found %v", value.wantRecordsByteStr[i], rec.(*row.VarLengthRecord).ToByte())
		}
	}

	txn := ctx.CurrentTxn()
	txn.unlockAll()
}

func TestFilterRecords(t *testing.T) {
	type valType struct {
		given              Block
		wantRecordsByteStr [][]byte
	}

	blk := Block{
		size:        107,
		recLocation: []BlockLocationPair{{row.NewLocationPair(0, 58), &st.Lock{}}, {row.NewLocationPair(58, 34), &st.Lock{}}},
		records:     []byte("127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasyou120\n0,2:3,2:6,4:11,2\n12:34:1467:56"),
	}
	block, _ := NewBlock(blk.ToByte(), 0, 1)
	value := valType{
		given: *block,
		wantRecordsByteStr: [][]byte{
			[]byte("127\n0,2:3,2:6,4:11,2:14,2:16,3:21,3\n12:34:1467:56\nitwasyou"),
			[]byte("120\n0,2:3,2:6,4:11,2\n12:34:1467:56"),
		},
	}

	cols := []column.Column{
		{Name: "id1", Type: column.INT},
		{Name: "id2", Type: column.INT},
		{Name: "id3", Type: column.INT},
		{Name: "id4", Type: column.INT},
		{Name: "id5", Type: column.STRING},
		{Name: "id6", Type: column.STRING},
		{Name: "id7", Type: column.STRING},
	}
	colData := row.NewColumnData_(cols)
	cfg := config.NewConfig(t.TempDir(), 1, 1)
	db := NewDB("test", cfg)
	ctx := GetClientContextMgr().NewClientCtx(cfg, db)
	recs, err := value.given.FilterRecords(ctx, colData, "id6", []byte("was"))
	if err != nil {
		t.Errorf("TestFilterRecords: %v", err)
	}

	if len(recs) != 1 {
		t.Errorf("TestFilterRecords: Expected length of records %v but found %v", 1, len(recs))
	}

	for i, rec := range recs {
		if !bytes.Equal(rec.(*row.VarLengthRecord).ToByte(), value.wantRecordsByteStr[i]) {
			t.Errorf("TestFilterRecords: Expected byte-string %v but found %v", value.wantRecordsByteStr[i], rec.(*row.VarLengthRecord).ToByte())
		}
	}

	txn := ctx.CurrentTxn()
	txn.unlockAll()
}

func TestUpdateFiteredRecords(t *testing.T) {
	type valType struct {
		given              *Block
		wantBlockSize      int
		wantRecordsByteStr []byte
	}

	blk := Block{
		size:        107,
		recLocation: []BlockLocationPair{{row.NewLocationPair(0, 58), &st.Lock{}}, {row.NewLocationPair(58, 34), &st.Lock{}}},
		records:     []byte("127\n0,2:3,2:6,4:11,2:14,2:16,3:19,3\n12:34:1467:56\nitwasyou120\n0,2:3,2:6,4:11,2\n12:34:1467:56"),
	}
	block, _ := NewBlock(blk.ToByte(), 0, 1)
	value := valType{
		given:              block,
		wantBlockSize:      106,
		wantRecordsByteStr: []byte("127\n0,2:3,2:6,4:11,2:14,2:16,2:18,3\n12:34:1467:56\nitbeyou120\n0,2:3,2:6,4:11,2\n12:34:1467:56"),
	}

	cols := []column.Column{
		{Name: "id1", Type: column.INT},
		{Name: "id2", Type: column.INT},
		{Name: "id3", Type: column.INT},
		{Name: "id4", Type: column.INT},
		{Name: "id5", Type: column.STRING},
		{Name: "id6", Type: column.STRING},
		{Name: "id7", Type: column.STRING},
	}
	colData := row.NewColumnData_(cols)
	cfg := config.NewConfig(t.TempDir(), 1, 1)
	db := NewDB("test", cfg)
	ctx := GetClientContextMgr().NewClientCtx(cfg, db)
	err := value.given.UpdateFiteredRecords(ctx, colData, "id6", []byte("was"), []byte("be"))
	if err != nil {
		t.Errorf("TestUpdateFiteredRecords: %v", err)
	}

	if bytes.Equal(block.records, value.wantRecordsByteStr) {
		t.Errorf("TestUpdateFiteredRecords: Expected byte-string %v but found %v", value.wantRecordsByteStr, block.records)
	}

	if value.wantBlockSize != block.size {
		t.Errorf("TestUpdateFiteredRecords: Expected block size %d but found %d", value.wantBlockSize, block.size)
	}

	txn := ctx.CurrentTxn()
	txn.unlockAll()
}

func TestUpdateRecords(t *testing.T) {
	type valType struct {
		given              *Block
		wantBlockSize      int
		wantRecordsByteStr []byte
	}

	blk := Block{
		size:        107,
		recLocation: []BlockLocationPair{{row.NewLocationPair(0, 58), &st.Lock{}}, {row.NewLocationPair(58, 34), &st.Lock{}}},
		records:     []byte("127\n0,2:3,2:6,4:11,2:14,2:16,3:19,3\n12:34:1467:56\nitwasyou120\n0,2:3,2:6,4:11,2\n12:34:1467:56"),
	}
	block, _ := NewBlock(blk.ToByte(), 0, 1)
	value := valType{
		given:              block,
		wantBlockSize:      110,
		wantRecordsByteStr: []byte("127\n0,2:3,2:6,4:11,2:14,2:16,6:22,3\n12:34:1467:56\nitwasn'tyou120\n0,2:3,2:6,4:11,2\n12:34:1467:56"),
	}

	cols := []column.Column{
		{Name: "id1", Type: column.INT},
		{Name: "id2", Type: column.INT},
		{Name: "id3", Type: column.INT},
		{Name: "id4", Type: column.INT},
		{Name: "id5", Type: column.STRING},
		{Name: "id6", Type: column.STRING},
		{Name: "id7", Type: column.STRING},
	}
	colData := row.NewColumnData_(cols)
	cfg := config.NewConfig(t.TempDir(), 1, 1)
	db := NewDB("test", cfg)
	ctx := GetClientContextMgr().NewClientCtx(cfg, db)
	err := value.given.UpdateRecords(ctx, colData, "id6", []byte("wasn't"))
	if err != nil {
		t.Errorf("TestUpdateRecords: %v", err)
	}

	if bytes.Equal(block.records, value.wantRecordsByteStr) {
		t.Errorf("TestUpdateRecords: Expected byte-string %v but found %v", value.wantRecordsByteStr, block.records)
	}

	if value.wantBlockSize != block.size {
		t.Errorf("TestUpdateRecords: Expected block size %d but found %d", value.wantBlockSize, block.size)
	}

	txn := ctx.CurrentTxn()
	txn.unlockAll()
}
