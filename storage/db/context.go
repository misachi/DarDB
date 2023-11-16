package db

import (
	"fmt"

	cfg "github.com/misachi/DarDB/config"
	// txn "github.com/misachi/DarDB/storage"
)

type ClientContext struct {
	currentTxn *Transaction
	txnMgr     *TransactionManager
	config     *cfg.Config
	database   *DB
}

func NewClientContext(cfg *cfg.Config, db *DB) (*ClientContext, error) {
	txnMgr := NewTxnManager()
	transaction, err := txnMgr.StartTransaction()
	if err != nil {
		return nil, fmt.Errorf("NewClientContext: Unable to create new transaction")
	}
	return &ClientContext{
		currentTxn: transaction,
		txnMgr:     txnMgr,
		config:     cfg,
		database:   db,
	}, nil
}

func (ctx *ClientContext) Close() {
	// CleanUp
	txn := ctx.currentTxn
	if txn.state == STARTED || txn.state == PENDING {
		txn.rollback()
	}
	ctx.txnMgr.EndTransaction(txn)
}

func (ctx *ClientContext) CurrentTxn() *Transaction {
	return ctx.currentTxn
}
