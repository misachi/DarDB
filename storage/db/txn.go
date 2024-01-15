package db

import (
	"fmt"
	"sync"

	st "github.com/misachi/DarDB/storage"
	row "github.com/misachi/DarDB/storage/db/row"
)

// type txn_t st.Txn_t

var TxnMgr *TransactionManager

const (
	PENDING = iota
	STARTED
	COMMITTED
	ABORTED
)

type TransactionManager struct {
	maxCommitId        st.Txn_t
	maxTxnID           st.Txn_t
	txnMgrMtx          *sync.Mutex
	ActiveTransactions []*Transaction
	DeleteTransactions []*Transaction
}

func NewTxnManager() *TransactionManager {
	if TxnMgr == nil {
		var txnID st.Txn_t
		var commitID st.Txn_t
		catalog := _Catalog
		if catalog != nil {
			if _, ok := catalog.db["catalog"]; ok {
				newTxnID := catalog.maxTblID.Add(1)
				catalog.SetMaxTxnId(st.Txn_t(newTxnID))
				txnID = catalog.MaxTxnId()

				newCommitID := catalog.maxCommitID.Add(1)
				catalog.SetMaxCommitId(st.Txn_t(newCommitID))
				commitID = catalog.MaxCommitId()
			}
		}
		TxnMgr = &TransactionManager{
			ActiveTransactions: make([]*Transaction, 0),
			DeleteTransactions: make([]*Transaction, 0),
			txnMgrMtx:          &sync.Mutex{},
			maxTxnID:           txnID,
			maxCommitId:        commitID,
		}
		return TxnMgr
	}
	return TxnMgr
}

func (tM TransactionManager) MaxCommitID() st.Txn_t {
	return tM.maxCommitId
}

func (tM TransactionManager) MaxTxnID() st.Txn_t {
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

func (t *TransactionManager) StartTransaction(ctx * ClientContext) (*Transaction, error) {
	newTxn := NewTransaction(ctx)
	// newTxn.commitId++
	// if newTxn.commitId <= 0 {
	// 	newTxn.transactionId = t.maxTxnID
	// } else {
	// 	newTxn.transactionId = newTxn.commitId
	// }

	newTxn.transactionId = t.maxTxnID
	newTxn.commitId = t.maxCommitId

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
	blockID  st.Blk_t
	tblID    st.Tbl_t
}

type Transaction struct {
	autocommit    bool
	transactionId st.Txn_t
	commitId      st.Txn_t
	state         int
	ctx           *ClientContext
	dataList      []transactionRecord
}

func NewTransaction(ctx *ClientContext) *Transaction {
	return &Transaction{state: PENDING, autocommit: false, ctx: ctx}
}

func (t Transaction) CommitID() st.Txn_t {
	return t.commitId
}

func (t Transaction) TransactionID() st.Txn_t {
	return t.transactionId
}

func (t Transaction) AutoCommit() bool {
	return t.autocommit
}

func (t *Transaction) SetAutocommit(autoCommit bool) {
	t.autocommit = autoCommit
}

func (t *Transaction) startTransaction(cID, tID st.Txn_t) (*Transaction, error) {
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
	_bufMgr := GetBufMgr()
	if bufMgr == nil {
		return fmt.Errorf("unlockAll: Unable to get buffer manager")
	}

	for _, lockedRecord := range t.dataList {
		path := fmt.Sprintf("%s/%s/%d", t.ctx.config.DataPath(), t.ctx.database.name, lockedRecord.tblID)
		blk, err := _bufMgr.GetBlock(path, lockedRecord.tblID, lockedRecord.blockID)
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

func (t *Transaction) TxnReadRecord(tblID st.Tbl_t, blockID st.Blk_t, loc row.LocationPair) error {
	t.dataList = append(t.dataList, transactionRecord{blockID: blockID, location: loc, tblID: tblID})
	return nil
}

// func (t *Transaction) TxnReadRecords(recs []row.Record) error {
// 	for _, rec := range recs {
// 		t.TxnReadRecord(rec)
// 	}
// 	return nil
// }

func (t *Transaction) TxnWriteRecord(tblID st.Tbl_t, blockID st.Blk_t, loc row.LocationPair) error {
	t.dataList = append(t.dataList, transactionRecord{blockID: blockID, location: loc, tblID: tblID})
	return nil
}
