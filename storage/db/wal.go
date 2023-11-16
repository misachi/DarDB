package db

import (
	"fmt"
	"unsafe"
)

type WALSTATE_t uint8
type wal_t uint64

const (
	WAL_START WALSTATE_t = 's'
	// INPROGRESS WALSTATE_t = 'p'
	WAL_COMMITTED WALSTATE_t = 'c'
	WAL_ABORTED   WALSTATE_t = 'a'
)

var CurrentWalSegment *WalSegment

type ETag struct {
	dbID    db_t
	tblID   tbl_t
	blockID blk_t
}

func NewETag(dbID db_t, tblID tbl_t, blkID blk_t) *ETag {
	return &ETag{dbID, tblID, blkID}
}

type Entry struct {
	state  WALSTATE_t
	txnID  txn_t
	tag    *ETag
	oldVal []byte
	newVal []byte
}

func NewEntry(txnID txn_t) *Entry {
	return &Entry{
		state:  WAL_START,
		txnID:  txnID,
		tag:    &ETag{},
		oldVal: []byte{},
		newVal: []byte{},
	}
}

func (e *Entry) InsertVal(oldVal, newVal []byte, tag *ETag) {
	e.oldVal = oldVal
	e.newVal = newVal
	e.tag = tag
}

func (e Entry) Size() wal_t {
	return wal_t(unsafe.Sizeof(e.state)) + wal_t(unsafe.Sizeof(e.txnID)) + wal_t(unsafe.Sizeof(e.tag)) + wal_t(len(e.oldVal)) + wal_t(len(e.newVal))
}

type WalSegment struct {
	WalID    uint32
	Size     wal_t
	EntryBuf []*Entry
}

func NewWalSegment(txnID txn_t) *WalSegment {
	var currentWalNum = txnID & 0xffffffff
	currentWalNum += 1
	walSeg := &WalSegment{
		WalID:    uint32(currentWalNum),
		EntryBuf: []*Entry{},
	}
	if CurrentWalSegment == nil {
		CurrentWalSegment = walSeg
	}
	return walSeg
}

func (wal *WalSegment) WalLog(ctx *ClientContext, entry *Entry) error {
	if wal.Size < wal_t(ctx.config.WalBufferSize()) {
		return fmt.Errorf("WalLog: WAL Segment full")
	}
	wal.EntryBuf = append(wal.EntryBuf, entry)
	wal.Size = entry.Size()
	return nil
}

func CurrentWal() *WalSegment {
	return CurrentWalSegment
}
