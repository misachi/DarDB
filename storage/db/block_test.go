package db

import (
	"bytes"
	"testing"
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
	data := []byte("123\n0,20:21,50\n0000000000000000000000000000000000000000000000000")
	block := Block{
		size:        123,
		recLocation: []BlockLocationPair{{row.NewLocationPair(0, 20), &st.Lock{}}, {row.NewLocationPair(21, 50), &st.Lock{}}},
		records:     []byte("0000000000000000000000000000000000000000000000000"),
	}
	newBlock, err := NewBlock(data, 1, 0)
	if err != nil {
		t.Error(err)
	}
	if newBlock.size != block.size {
		t.Errorf("TestNewBlock: Expected size to be %d but found %d", block.size, newBlock.size)
	}

	if bytes.Equal(block.records, newBlock.records) {
		t.Errorf("TestNewBlock: Expected %s bytes in record but got %s", block.records, newBlock.records)
	}

	for i, loc := range block.recLocation {
		if loc.Offset() != newBlock.recLocation[i].Offset() || loc.Size() != newBlock.recLocation[i].Size() {
			t.Errorf("TestNewBlock: Expected locations to be %v but found %v", loc, newBlock.recLocation[i])
		}
	}
}
