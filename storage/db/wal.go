package db

import (
	"fmt"
	"unsafe"
	st "github.com/misachi/DarDB/storage"
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
	dbID    st.DB_t
	tblID   st.Tbl_t
	blockID st.Blk_t
}

func NewETag(dbID st.DB_t, tblID st.Tbl_t, blkID st.Blk_t) *ETag {
	return &ETag{dbID, tblID, blkID}
}

type Entry struct {
	state  WALSTATE_t
	txnID  st.Txn_t
	tag    *ETag
	oldVal []byte
	newVal []byte
}

func NewEntry(txnID st.Txn_t) *Entry {
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

func NewWalSegment(txnID st.Txn_t) *WalSegment {
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
