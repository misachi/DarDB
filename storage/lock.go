package storage

import (
	"fmt"
	"sync"
)

const (
	SHARED_LOCK = iota
	EXCLUSIVE_LOCK
	NO_LOCK
)

// type LockEntry struct {
// 	lockType int
// 	lock     *Lock
// 	value    interface{}
// }

// func NewLockEntry(lock *Lock, lType int, val interface{}) *LockEntry {
// 	return &LockEntry{
// 		lock:     lock,
// 		lockType: lType,
// 		value:    val,
// 	}
// }

// func (e *LockEntry) UnlockEntry() error {
// 	if e.lockType == EXCLUSIVE_LOCK {
// 		e.lock.releaseExclusiveLock()
// 	} else if e.lockType == SHARED_LOCK {
// 		e.lock.releaseSharedLock()
// 	} else {
// 		return fmt.Errorf("UnlockEntry: unknown lock type")
// 	}
// 	return nil
// }

type Lock struct {
	sharedLockCount uint
	lockType        int
	eLock           *sync.Mutex
}

func NewLock() *Lock {
	return &Lock{
		sharedLockCount: 0,
		eLock:           &sync.Mutex{},
	}
}

func (l *Lock) acquireSharedLock() error {
	l.sharedLockCount++
	return nil
}

func (l *Lock) releaseSharedLock() error {
	l.sharedLockCount--
	return nil
}

func (l *Lock) acquireExclusiveLock() error {
	l.eLock.Lock()
	return nil
}

func (l *Lock) releaseExclusiveLock() error {
	l.eLock.Unlock()
	return nil
}
