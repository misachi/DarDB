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

type Lock struct {
	sharedLockCount uint
	lockType        int
	eLock           *sync.RWMutex
}

func NewLock() *Lock {
	return &Lock{
		sharedLockCount: 0,
		eLock:           &sync.RWMutex{},
	}
}

func (l *Lock) AcquireLock(mode int) error {
	if mode == EXCLUSIVE_LOCK {
		l.acquireExclusiveLock()
	} else if mode == SHARED_LOCK {
		l.acquireSharedLock()
	} else {
		return fmt.Errorf("UnlockEntry: unknown lock type")
	}
	return nil
}

func (l *Lock) ReleaseLock() error {
	if l.lockType == EXCLUSIVE_LOCK {
		l.releaseExclusiveLock()
	} else if l.lockType == SHARED_LOCK {
		l.releaseSharedLock()
	} else {
		return fmt.Errorf("UnlockEntry: unknown lock type")
	}
	return nil
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
