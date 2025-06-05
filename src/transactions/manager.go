package transactions

import (
	"runtime"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
)

type Manager struct {
	qsGuard sync.Mutex
	qs      map[RecordID]*txnQueue

	lockedRecordsGuard sync.Mutex
	lockedRecords      map[TxnID]map[RecordID]struct{}
}

func NewManager() *Manager {
	return &Manager{
		qsGuard:            sync.Mutex{},
		qs:                 map[RecordID]*txnQueue{},
		lockedRecordsGuard: sync.Mutex{},
		lockedRecords:      map[TxnID]map[RecordID]struct{}{},
	}
}

func (m *Manager) Lock(r txnLockRequest) <-chan struct{} {
	q := func() *txnQueue {
		m.qsGuard.Lock()
		defer m.qsGuard.Unlock()

		q, ok := m.qs[r.recordId]
		if !ok {
			q = newTxnQueue()
			m.qs[r.recordId] = q
		}

		return q
	}()

	notifier := q.Lock(r)
	if notifier == nil {
		return nil
	}

	func() {
		m.lockedRecordsGuard.Lock()
		defer m.lockedRecordsGuard.Unlock()

		alreadyLockedRecords, ok := m.lockedRecords[r.txnID]
		if !ok {
			alreadyLockedRecords = make(map[RecordID]struct{})
			m.lockedRecords[r.txnID] = alreadyLockedRecords
		}

		_, isAleadyLocked := alreadyLockedRecords[r.recordId]
		assert.Assert(!isAleadyLocked,
			"Didn't expect the record %+v to be locked by a transaction %+v",
			r.recordId,
			r.txnID)

		alreadyLockedRecords[r.recordId] = struct{}{}
	}()

	return notifier
}

// Upgrade attempts to upgrade the lock held by the transaction specified in the txnLockRequest `r`.
// It checks that the lock is currently held and that the transaction is eligible for an upgrade.
// If the upgrade cannot be performed immediately (due to lock contention), it returns nil and the caller should retry.
// If the upgrade can proceed, it inserts a new entry into the transaction queue and returns a channel that will be closed
// when the upgrade is granted. The function ensures proper synchronization and queue manipulation to maintain lock order and safety.
//
// Parameters:
//   - r: txnLockRequest containing the transaction and record identifiers for the upgrade request.
//
// Returns:
//   - <-chan struct{}: A channel that will be closed when the lock upgrade is granted, or nil if the upgrade cannot be performed immediately.
func (m *Manager) Upgrade(r txnLockRequest) <-chan struct{} {
	q := func() *txnQueue {
		m.qsGuard.Lock()
		defer m.qsGuard.Unlock()

		q, present := m.qs[r.recordId]
		assert.Assert(present,
			"trying to upgrade a lock on the unlocked tuple. request: %+v",
			r)

		return q
	}()

	q.mu.Lock()
	cur, exists := q.txnNodes[r.txnID]
	q.mu.Unlock()

	assert.Assert(exists, "transaction %+v hasn't acquired the tuple with %+v record id. request: %+v", r.txnID, r.recordId, r)
	cur.mu.Lock()
	assert.Assert(cur.isAcquired, "can't upgrade a lock: it wasn't acquired yet. request: %+v", r)

	first := cur.prev
	if !first.mu.TryLock() {
		cur.mu.Unlock()
		return nil // the caller should retry
	}
	defer first.mu.Unlock()

	next := cur.next
	next.mu.Lock()

	for next.isAcquired {
		tmp := next.next
		tmp.mu.Lock()
		cur.mu.Unlock()
		cur = next
		next = tmp
	}

	c := make(chan struct{})
	e := &txnQueueEntry{
		r:          r,
		notifier:   c,
		isAcquired: false,
		mu:         sync.Mutex{},
		next:       next,
		prev:       cur,
	}

	cur.next = e
	next.prev = e

	q.mu.Lock()
	q.txnNodes[r.txnID] = e
	q.mu.Unlock()

	cur.mu.Unlock()
	next.mu.Unlock()

	second := first.next
	second.mu.Lock()
	defer second.mu.Unlock()

	third := second.next
	third.mu.Lock()
	defer third.mu.Unlock()

	first.next = third
	third.prev = first

	return c
}

// Unlock releases the lock held by a transaction on a specific record.
// It first retrieves the transaction queue associated with the record ID,
// ensuring that the record is currently locked. It then attempts to unlock
// the record, retrying if necessary until successful. After unlocking,
// it removes the record from the set of records locked by the transaction.
// Panics if the record is not currently locked or if the transaction does not
// have any locked records.
func (m *Manager) Unlock(r txnUnlockRequest) {
	q := func() *txnQueue {
		m.qsGuard.Lock()
		defer m.qsGuard.Unlock()

		q, present := m.qs[r.recordId]
		assert.Assert(present,
			"trying to unlock the already unlocked tuple. recordID: %+v",
			r.recordId)

		return q
	}()

	for !q.Unlock(r) {
		// TODO: rething the retries
		runtime.Gosched()
	}

	func() {
		m.lockedRecordsGuard.Lock()
		defer m.lockedRecordsGuard.Unlock()

		lockedRecords, lockedRecordsExist := m.lockedRecords[r.txnID]
		assert.Assert(lockedRecordsExist,
			"expected a set of locked records for the transaction %+v to exist",
			r.txnID,
		)
		delete(lockedRecords, r.recordId)
	}()
}

func (m *Manager) UnlockAll(TransactionID TxnID) {
	lockedRecords := func() map[RecordID]struct{} {
		m.lockedRecordsGuard.Lock()
		defer m.lockedRecordsGuard.Unlock()

		lockedRecords, ok := m.lockedRecords[TransactionID]
		assert.Assert(ok,
			"expected a set of locked records for the transaction %+v to exist",
			TransactionID)
		delete(m.lockedRecords, TransactionID)

		return lockedRecords
	}()

	unlockRequest := txnUnlockRequest{
		txnID: TransactionID,
	}

	for r := range lockedRecords {
		q := func() *txnQueue {
			m.qsGuard.Lock()
			defer m.qsGuard.Unlock()

			q, present := m.qs[r]
			assert.Assert(present,
				"trying to unlock a transaction on an unlocked tuple. recordID: %+v",
				r)

			return q
		}()

		unlockRequest.recordId = r
		for !q.Unlock(unlockRequest) {
			runtime.Gosched()
		}
	}
}
