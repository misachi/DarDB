package storage

import (
	"bytes"
	"testing"
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
		recLocation: []LocationPair{{0, 20}, {21, 50}},
		records:     []byte("0000000000000000000000000000000000000000000000000"),
	}
	newBlock, err := NewBlock(data, 1)
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
		if loc.offset != newBlock.recLocation[i].offset || loc.size != newBlock.recLocation[i].size {
			t.Errorf("TestNewBlock: Expected locations to be %v but found %v", loc, newBlock.recLocation[i])
		}
	}
}
