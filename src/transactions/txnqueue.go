package transactions

import (
	"math"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
)

type txnQueueEntry struct {
	r          txnLockRequest
	notifier   chan struct{}
	isAcquired bool

	mu   sync.Mutex
	next *txnQueueEntry
	prev *txnQueueEntry
}

// SafeNext safely advances to the next txnQueueEntry in the queue.
// It acquires the lock on the next entry before releasing the lock on the current entry,
// ensuring that the transition between entries is thread-safe and prevents race conditions.
// Returns a pointer to the next txnQueueEntry.
func (lockedEntry *txnQueueEntry) SafeNext() *txnQueueEntry {
	next := lockedEntry.next
	assert.Assert(next != nil, "precondition is violated")

	next.mu.Lock()
	lockedEntry.mu.Unlock()

	return next
}

// SafeInsert inserts the given txnQueueEntry 'n' immediately after the current (locked) entry in the queue.
// It updates the necessary pointers to maintain the doubly-linked list structure.
// The method assumes that the current entry is already locked, and it locks the 'next' entry
// to safely update its 'prev' pointer, ensuring thread safety during the insertion.
func (lockedEntry *txnQueueEntry) SafeInsert(n *txnQueueEntry) {
	next := lockedEntry.next

	n.prev = lockedEntry
	n.next = next

	lockedEntry.next = n

	next.mu.Lock()
	next.prev = n
	next.mu.Unlock()
}

type txnQueue struct {
	head *txnQueueEntry
	tail *txnQueueEntry

	mu       sync.Mutex
	txnNodes map[TxnID]*txnQueueEntry
}

// processBatch processes a batch of transactions in the txnQueue starting from the given
// muGuardedHead entry. It iterates through the queue, granting locks to transactions whose
// lock modes are compatible with all previously granted lock modes in the current batch.
// For each compatible transaction, it marks the entry as acquired and notifies the waiting
// transaction by closing its notifier channel. The function ensures that only a prefix of
// the queue is in the locked state at any time, and stops processing when an incompatible
// lock mode is encountered or the end of the queue is reached.
//
// Preconditions:
//   - muGuardedHead must not be already acquired (isAcquired == false).
//   - muGuardedHead.mu must be locked by the caller.
//
// Postconditions:
//   - All compatible transactions in the batch are granted the lock and notified.
//   - Only the processed prefix of the queue is in the locked state.
func (q *txnQueue) processBatch(muGuardedHead *txnQueueEntry) {
	assert.Assert(!muGuardedHead.isAcquired, "processBatch contract is violated")

	cur := muGuardedHead
	defer func() { cur.mu.Unlock() }()

	if cur == q.tail {
		return
	}

	seenLockModes := make(map[LockMode]struct{})
outer:
	for {
		for seenMode := range seenLockModes {
			if !compatibleLockModes(seenMode, cur.r.lockMode) {
				break outer
			}
		}
		seenLockModes[cur.r.lockMode] = struct{}{}

		cur.isAcquired = true
		close(cur.notifier) // grants the lock to the transaction

		if cur.next == q.tail {
			break
		}

		cur = cur.SafeNext()
		assert.Assert(!cur.isAcquired, "only list prefix is allowed to be in the locked state")
	}
}

func newTxnQueue() *txnQueue {
	head := &txnQueueEntry{
		r: txnLockRequest{
			txnID:    math.MaxUint64, // Needed for the deadlock prevention policy
			lockMode: helper_ALLOW_ALL,
		},
	}
	tail := &txnQueueEntry{
		r: txnLockRequest{
			txnID:    0, // Needed for the deadlock prevention policy
			lockMode: helper_FORBID_ALL,
		},
	}
	head.next = tail
	tail.prev = head

	q := &txnQueue{
		head: head,
		tail: tail,

		mu:       sync.Mutex{},
		txnNodes: map[TxnID]*txnQueueEntry{},
	}

	return q
}

// Lock attempts to acquire a lock for the given transaction lock request `r` in the transaction queue.
// It enforces a deadlock prevention policy where only older transactions can wait for younger ones;
// if a younger transaction attempts to wait for an older one, it is aborted (returns nil).
//
// If the requested lock is compatible with all currently held locks, the lock is granted immediately
// and a closed channel is returned. Otherwise, the request is queued and a channel is returned that
// will be closed when the lock is eventually granted. The returned channel can be used to wait for
// lock acquisition.
func (q *txnQueue) Lock(r txnLockRequest) <-chan struct{} {
	q.head.mu.Lock()

	cur := q.head
	defer func(c **txnQueueEntry) { (*c).mu.Unlock() }(&cur)

	locksAreCompatible := true

	for {
		assert.Assert(cur.r.txnID != r.txnID, "trying to lock already locked transaction. %+v", r)

		locksAreCompatible = locksAreCompatible && compatibleLockModes(r.lockMode, cur.r.lockMode)
		if !locksAreCompatible && cur.r.txnID < r.txnID {
			// Deadlock prevention policy
			// Only an older transaction can wait for a younger one.
			// Ohterwise, a younger transaction is aborted
			return nil
		}

		if cur.next == q.tail {
			break
		}

		cur = cur.SafeNext()
	}

	notifier := make(chan struct{})
	if locksAreCompatible {
		close(notifier) // Grant the lock immediately
		newNode := &txnQueueEntry{
			r:          r,
			notifier:   nil,
			isAcquired: true,
		}
		cur.SafeInsert(newNode)

		q.mu.Lock()
		q.txnNodes[r.txnID] = newNode
		q.mu.Unlock()

		return notifier
	}

	newNode := &txnQueueEntry{
		r:          r,
		notifier:   notifier,
		isAcquired: false,
	}
	cur.SafeInsert(newNode)

	q.mu.Lock()
	q.txnNodes[r.txnID] = newNode
	q.mu.Unlock()

	return notifier
}

func (q *txnQueue) Unlock(r txnUnlockRequest) bool {
	q.mu.Lock()
	deletingNode, present := q.txnNodes[r.txnID]
	q.mu.Unlock()

	assert.Assert(present, "node not found. %+v", r)

	deletingNode.mu.Lock()
	defer deletingNode.mu.Unlock()

	prev := deletingNode.prev
	// TODO: rework this into something NOT using retries
	// Potential solution: tombstone marker.
	// deleted nodes then would be cleaned up during the
	// next insert operation
	if !prev.mu.TryLock() {
		return false
	}

	q.mu.Lock()
	delete(q.txnNodes, r.txnID)
	q.mu.Unlock()

	next := deletingNode.next
	next.mu.Lock()
	next.prev = prev
	next.mu.Unlock()

	prev.next = next
	if deletingNode.isAcquired && prev == q.head && !next.isAcquired {
		q.processBatch(prev.SafeNext())
		return true
	}
	prev.mu.Unlock()

	return true
}
