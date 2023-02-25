package transaction

import (
	"sync"

	ds "github.com/misachi/DarDB/structure"
)

type txn_t uint64

const (
	STARTED = iota
	COMMITTED
	ABORTED
)

type TransactionManager struct {
	activeTransactions *ds.List
	deleteTransactions *ds.List
	currentTimestamp   txn_t
	currentTransaction *Transaction
	mut                *sync.Mutex
}

type Transaction struct {
	transactionId txn_t
	commitId      txn_t
	state         int
	dataList      *ds.List
}

func (t *Transaction) Commit() {}

func (t *Transaction) Rollback() {}

// Cortec - he
// MobiKash
