package transactions

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestManagerBasicOperation(t *testing.T) {
	m := &Manager{qs: make(map[RecordID]*txnQueue)}

	// Test queue creation on first lock
	req := txnLockRequest{txnId: 1, recordId: 100, lockMode: SHARED}
	notifier := m.Lock(req)
	expectClosedChannel(t, notifier, "Initial lock should be granted")

	// Verify queue exists
	m.mu.Lock()
	if _, exists := m.qs[100]; !exists {
		t.Error("Manager should create queue for new record ID")
	}
	m.mu.Unlock()

	// Successful unlock
	m.Unlock(txnUnlockRequest{txnId: 1, recordId: 100})

	// Verify queue persists after unlock
	m.mu.Lock()
	if _, exists := m.qs[100]; !exists {
		t.Error("Queue should remain after unlock")
	}
	m.mu.Unlock()
}

func TestManagerConcurrentRecordAccess(t *testing.T) {
	m := &Manager{qs: make(map[RecordID]*txnQueue)}
	var wg sync.WaitGroup

	for i := range 5 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			recordID := RecordID(id % 1) // Two distinct records
			req := txnLockRequest{
				txnId:    TransactionID(id),
				recordId: recordID,
				lockMode: SHARED,
			}
			fmt.Printf("before lock %d\n", id)
			notifier := m.Lock(req)
			fmt.Printf("after lock %d\n", id)
			expectClosedChannel(t, notifier, "Concurrent access to different records should work")

			fmt.Printf("before unlock %d\n", id)
			m.Unlock(txnUnlockRequest{txnId: TransactionID(id), recordId: recordID})
			fmt.Printf("after unlock %d\n", id)
		}(i)
	}

	wg.Wait()
}

func TestManagerUnlockPanicScenarios(t *testing.T) {
	m := &Manager{qs: make(map[RecordID]*txnQueue)}

	// Test non-existent record panic
	t.Run("NonExistentRecord", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for non-existent record")
			}
		}()
		m.Unlock(txnUnlockRequest{txnId: 1, recordId: 999})
	})

	// Test double unlock panic
	t.Run("DoubleUnlock", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for double unlock")
			}
		}()

		req := txnLockRequest{txnId: 1, recordId: 200, lockMode: EXCLUSIVE}
		notifier := m.Lock(req)
		expectClosedChannel(t, notifier, "Lock should be granted")
		m.Unlock(txnUnlockRequest{txnId: 1, recordId: 200})
		m.Unlock(txnUnlockRequest{txnId: 1, recordId: 200}) // Panic here
	})
}

func TestManagerLockContention(t *testing.T) {
	m := &Manager{qs: make(map[RecordID]*txnQueue)}
	recordID := RecordID(300)

	// First exclusive lock
	req1 := txnLockRequest{txnId: 5, recordId: recordID, lockMode: EXCLUSIVE}
	notifier1 := m.Lock(req1)
	expectClosedChannel(t, notifier1, "First exclusive lock should be granted")

	// Second exclusive lock (should block)
	req2 := txnLockRequest{txnId: 4, recordId: recordID, lockMode: EXCLUSIVE}
	notifier2 := m.Lock(req2)
	expectOpenChannel(t, notifier2, "Second exclusive lock should block")

	// Concurrent shared lock (should also block)
	req3 := txnLockRequest{txnId: 3, recordId: recordID, lockMode: SHARED}
	notifier3 := m.Lock(req3)
	expectOpenChannel(t, notifier3, "Shared lock should block behind exclusive")

	// Unlock first and verify chain
	m.Unlock(txnUnlockRequest{txnId: 5, recordId: recordID})
	expectClosedChannel(t, notifier2, "Second lock should be granted after unlock")
	m.Unlock(txnUnlockRequest{txnId: 4, recordId: recordID})
	expectClosedChannel(t, notifier3, "Shared lock should be granted after exclusives")
}

func TestManagerUnlockRetry(t *testing.T) {
	m := &Manager{qs: make(map[RecordID]*txnQueue)}
	recordID := RecordID(400)

	// Setup lock
	req := txnLockRequest{txnId: 1, recordId: recordID, lockMode: EXCLUSIVE}
	notifier := m.Lock(req)
	expectClosedChannel(t, notifier, "Lock should be granted")

	// Block the unlock path
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// Acquire lock on previous node to force retry
		m.qs[recordID].head.mu.Lock()
		time.Sleep(50 * time.Millisecond) // Hold lock briefly
		m.qs[recordID].head.mu.Unlock()
		wg.Done()
	}()

	// This should retry until successful
	m.Unlock(txnUnlockRequest{txnId: 1, recordId: recordID})
	wg.Wait()
}
