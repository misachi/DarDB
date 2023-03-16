package transaction

import (
	"fmt"

	blk "github.com/misachi/DarDB/storage"
)

type txn_t uint64

const (
	PENDING = iota
	STARTED
	COMMITTED
	ABORTED
)

type TransactionManager struct {
	ActiveTransactions []*Transaction
	DeleteTransactions []*Transaction
	maxCommitId        txn_t
	maxTxnID           txn_t
}

func NewTxnManager() *TransactionManager {
	return &TransactionManager{
		ActiveTransactions: make([]*Transaction, 0),
		DeleteTransactions: make([]*Transaction, 0),
	}
}

func (tM TransactionManager) MaxCommitID() txn_t {
	return tM.maxCommitId
}

func (tM TransactionManager) MaxTxnID() txn_t {
	return tM.maxTxnID
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
	t.ActiveTransactions = append(t.ActiveTransactions, txn)
	return txn, nil
}

func (t *TransactionManager) Commit(txn *Transaction) {}

func (t *TransactionManager) Rollback(txn *Transaction) {}

type Transaction struct {
	autocommit    bool
	transactionId txn_t
	commitId      txn_t
	state         int
	DataList      []blk.Record
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

func (t *Transaction) unlockAll() {
	for _, rec := range t.DataList {
		rec.UnLockRecord()
	}
}

// func (t *Transaction) commit() error {}

// func (t *Transaction) rollback() error {}
