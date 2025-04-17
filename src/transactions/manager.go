package transactions

import (
	"runtime"
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
	defer m.mu.Unlock()

	q, present := m.qs[r.recordId]
	Assert(present, "trying to unlock a transaction on an unlocked tuple")

	for !q.Unlock(r) {
		runtime.Gosched() // TODO: rething the retries
	}
}
