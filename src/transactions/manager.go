package transactions

import (
	"fmt"
	"sync"
)

type Manager struct {
	mu sync.Mutex
	qs map[RecordID]*txnQueue
}

func (m *Manager) Lock(r txnLockRequest) <-chan struct{} {
	m.mu.Lock()
	q, ok := m.qs[r.recordId]
	if !ok {
		q = newTxnQueue()
		m.qs[r.recordId] = q
	}
	m.mu.Unlock()

	return q.Lock(r)
}

func (m *Manager) Unlock(r txnUnlockRequest) {
	m.mu.Lock()
	q, present := m.qs[r.recordId]
	m.mu.Unlock()
	Assert(present, "trying to unlock a transaction on an unlocked tuple")

	for !q.Unlock(r) {
		fmt.Println("FAIL")
		// runtime.Gosched() // TODO: rething the retries
	}
}
