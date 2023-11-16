package db

import (
	"fmt"
	"sync"

	row "github.com/misachi/DarDB/storage/db/row"
)

type txn_t uint64

var TxnMgr *TransactionManager

const (
	PENDING = iota
	STARTED
	COMMITTED
	ABORTED
)

type TransactionManager struct {
	maxCommitId        txn_t
	maxTxnID           txn_t
	txnMgrMtx          *sync.Mutex
	ActiveTransactions []*Transaction
	DeleteTransactions []*Transaction
}

func NewTxnManager() *TransactionManager {
	if TxnMgr == nil {
		TxnMgr = &TransactionManager{
			ActiveTransactions: make([]*Transaction, 0),
			DeleteTransactions: make([]*Transaction, 0),
			txnMgrMtx:          &sync.Mutex{},
		}
		return TxnMgr
	}
	return TxnMgr
}

func (tM TransactionManager) MaxCommitID() txn_t {
	return tM.maxCommitId
}

func (tM TransactionManager) MaxTxnID() txn_t {
	return tM.maxTxnID
}

func (tM *TransactionManager) EndTransaction(transaction *Transaction) {
	for idx, txn := range tM.ActiveTransactions {
		if txn.transactionId == transaction.transactionId {
			tM.ActiveTransactions = append(tM.ActiveTransactions[:idx], tM.ActiveTransactions[idx+1:]...)
			tM.DeleteTransactions = append(tM.DeleteTransactions, txn)
			break
		}
	}
}

func (t *TransactionManager) StartTransaction() (*Transaction, error) {
	newTxn := NewTransaction()
	newTxn.commitId++
	if newTxn.commitId <= 0 {
		newTxn.transactionId++
	} else {
		newTxn.transactionId = newTxn.commitId
	}

	txn, err := newTxn.startTransaction(newTxn.commitId, newTxn.transactionId)
	if err != nil {
		return nil, fmt.Errorf("StartTransaction: unable to create new transaction")
	}
	t.txnMgrMtx.Lock()
	defer t.txnMgrMtx.Unlock()
	t.ActiveTransactions = append(t.ActiveTransactions, txn)
	return txn, nil
}

func (t *TransactionManager) Commit(txn *Transaction) {}

func (t *TransactionManager) Rollback(txn *Transaction) {}

type transactionRecord struct {
	location row.LocationPair
	blockID  blk_t
}

type Transaction struct {
	autocommit    bool
	transactionId txn_t
	commitId      txn_t
	state         int
	dataList      []transactionRecord
}

func NewTransaction() *Transaction {
	return &Transaction{state: PENDING, autocommit: false}
}

func (t Transaction) CommitID() txn_t {
	return t.commitId
}

func (t Transaction) TransactionID() txn_t {
	return t.transactionId
}

func (t Transaction) AutoCommit() bool {
	return t.autocommit
}

func (t *Transaction) SetAutocommit(autoCommit bool) {
	t.autocommit = autoCommit
}

func (t *Transaction) startTransaction(cID, tID txn_t) (*Transaction, error) {
	switch t.state {
	case PENDING:
		return nil, fmt.Errorf("startTransaction: Transaction already started")
	case COMMITTED:
		return nil, fmt.Errorf("startTransaction: Transaction already committed")
	case ABORTED:
		return nil, fmt.Errorf("startTransaction: Transaction has been aborted")
	}

	t.commitId = cID
	t.transactionId = tID
	t.state = STARTED
	return t, nil
}

func (t *Transaction) unlockAll() error {
	_bufMgr := getBufMgr()
	if bufMgr == nil {
		return fmt.Errorf("unlockAll: Unable to get buffer manager")
	}
	for _, lockedRecord := range t.dataList {
		blk, err := _bufMgr.GetBlock(int64(lockedRecord.blockID))
		if err != nil {
			return fmt.Errorf("unlockAll: GetBlock error: %v", err)
		}
		for idx, loc := range blk.recLocation {
			if loc.Offset() == lockedRecord.location.Offset() && loc.Size() == lockedRecord.location.Size() {
				blk.recLocation[idx].lockField.ReleaseLock()
			}
		}
	}
	return nil
}

func (t *Transaction) transactionAbort() {
	t.state = ABORTED
	t.rollback()
}

func (t *Transaction) commit() error {
	// TODO: Write to WAL
	t.state = COMMITTED
	if err := t.unlockAll(); err != nil {
		return fmt.Errorf("commit error: %v", err)
	}
	return nil
}

func (t *Transaction) rollback() error {
	t.state = ABORTED
	if err := t.unlockAll(); err != nil {
		return fmt.Errorf("rollback error: %v", err)
	}
	return nil
}

func (t *Transaction) TxnReadRecord(blockID blk_t, loc row.LocationPair) error {
	t.dataList = append(t.dataList, transactionRecord{blockID: blockID, location: loc})
	return nil
}

// func (t *Transaction) TxnReadRecords(recs []row.Record) error {
// 	for _, rec := range recs {
// 		t.TxnReadRecord(rec)
// 	}
// 	return nil
// }

func (t *Transaction) TxnWriteRecord(blockID blk_t, loc row.LocationPair) error {
	t.dataList = append(t.dataList, transactionRecord{blockID: blockID, location: loc})
	return nil
}
