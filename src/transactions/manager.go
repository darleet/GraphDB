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

func (m *Manager) Upgrade(r txnLockUpgradeRequest) {
	q := func() *txnQueue {
		m.qsGuard.Lock()
		defer m.qsGuard.Unlock()

		q, present := m.qs[r.recordId]
		assert.Assert(present,
			"trying to upgrade a lock on the unlocked tuple. recordID: %+v",
			r.recordId)

		return q
	}()

	entry, ok := q.txnNodes[r.txnID]
	assert.Assert(ok, "transaction %+v hasn't acquired the tuple with %+v record id", r.txnID, r.recordId)
	

}

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
