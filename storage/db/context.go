package db

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	cfg "github.com/misachi/DarDB/config"
	// txn "github.com/misachi/DarDB/storage"
)

var ClientCtxMgr *ClientContextMgr

type ClientContextMgr struct {
	maxID   atomic.Uint32
	context map[uint32]*ClientContext
	mtx     *sync.RWMutex
}

func NewClientContextMgr() *ClientContextMgr {
	return &ClientContextMgr{
		context: make(map[uint32]*ClientContext),
		mtx:     &sync.RWMutex{},
	}
}

func GetClientContextMgr() *ClientContextMgr {
	if ClientCtxMgr != nil {
		return ClientCtxMgr
	}
	ClientCtxMgr := NewClientContextMgr()
	return ClientCtxMgr
}

func (ctxMgr *ClientContextMgr) NewClientCtx(cfg *cfg.Config, db *DB) *ClientContext {
	newID := ctxMgr.maxID.Add(1)
	ctx, err := NewClientContext(newID, cfg, db)
	if err != nil {
		slog.Error("NewClientCtx: unable to create new context")
		panic(err)
	}
	ctxMgr.context[newID] = ctx
	return ctx
}

type ClientContext struct {
	ctxID      uint32
	currentTxn *Transaction
	txnMgr     *TransactionManager
	config     *cfg.Config
	database   *DB
}

func NewClientContext(ctxID uint32, cfg *cfg.Config, db *DB) (*ClientContext, error) {
	txnMgr := NewTxnManager()
	ctx := &ClientContext{}
	transaction, err := txnMgr.StartTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("NewClientContext: Unable to create new context: %v", err)
	}
	ctx.currentTxn = transaction
	ctx.txnMgr = txnMgr
	ctx.config = cfg
	ctx.database =db
	ctx.ctxID = ctxID
	return ctx, nil
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
