package transactions

import (
	"runtime"
	"sync"
)

type Manager struct {
	qsGuard sync.Mutex
	qs      map[RecordID]*txnQueue

	lockedRecordsGuard sync.Mutex
	lockedRecords      map[TransactionID]map[RecordID]struct{}
}

func NewManager() *Manager {
	return &Manager{
		qsGuard:            sync.Mutex{},
		qs:                 map[RecordID]*txnQueue{},
		lockedRecordsGuard: sync.Mutex{},
		lockedRecords:      map[TransactionID]map[RecordID]struct{}{},
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

		alreadyLockedRecords, ok := m.lockedRecords[r.txnId]
		if !ok {
			alreadyLockedRecords = make(map[RecordID]struct{})
			m.lockedRecords[r.txnId] = alreadyLockedRecords
		}

		_, isAlraedyLocked := alreadyLockedRecords[r.recordId]
		Assert(!isAlraedyLocked,
			"Didn't expect the record %+v to be locked by a transaction %+v",
			r.recordId,
			r.txnId)

		alreadyLockedRecords[r.recordId] = struct{}{}
	}()
	return notifier
}

func (m *Manager) Unlock(r txnUnlockRequest) {
	q := func() *txnQueue {
		m.qsGuard.Lock()
		defer m.qsGuard.Unlock()

		q, present := m.qs[r.recordId]
		Assert(present,
			"trying to unlock a transaction on an unlocked tuple. recordID: %+v",
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

		lockedRecords, lockedRecordsExist := m.lockedRecords[r.txnId]
		Assert(lockedRecordsExist,
			"expected a set of locked records for the transaction %+v to exist",
			r.txnId,
		)
		delete(lockedRecords, r.recordId)
	}()
}

func (m *Manager) UnlockAll(txnId TransactionID) {
	lockedRecords := func() map[RecordID]struct{} {
		m.lockedRecordsGuard.Lock()
		defer m.lockedRecordsGuard.Unlock()

		lockedRecords, ok := m.lockedRecords[txnId]
		Assert(ok,
			"expected a set of locked records for the transaction %+v to exist",
			txnId)
		delete(m.lockedRecords, txnId)
		return lockedRecords
	}()

	unlockRequest := txnUnlockRequest{
		txnId: txnId,
	}
	for r := range lockedRecords {
		q := func() *txnQueue {
			m.qsGuard.Lock()
			defer m.qsGuard.Unlock()

			q, present := m.qs[r]
			Assert(present,
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
