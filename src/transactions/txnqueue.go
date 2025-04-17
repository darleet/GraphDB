package transactions

import (
	"math"
	"sync"
)

type txnQueueEntry struct {
	r        txnLockRequest
	notifier chan struct{}
	isLocked bool

	mu   sync.Mutex
	next *txnQueueEntry
	prev *txnQueueEntry
}

func (lockedEntry *txnQueueEntry) SafeNext() *txnQueueEntry {
	next := lockedEntry.next
	next.mu.Lock()
	lockedEntry.mu.Unlock()
	return next
}

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
	txnNodes map[TransactionID]*txnQueueEntry
}

func (q *txnQueue) processBatch(lockedHead *txnQueueEntry) {
	seenLockModes := make(map[LockMode]struct{})

	cur := lockedHead.SafeNext()
	if cur == q.tail {
		cur.mu.Unlock()
		return
	}

	for {
		Assert(!cur.isLocked, "only list prefix is allowed to be in the locked state")

		for seenMode := range seenLockModes {
			if !compatibleLockModes(seenMode, cur.r.lockMode) {
				cur.mu.Unlock()
				break
			}
		}
		seenLockModes[cur.r.lockMode] = struct{}{}

		cur.isLocked = true
		close(cur.notifier) // grants the lock to a transaction

		if cur.next == q.tail {
			break
		}
		cur = cur.SafeNext()
	}

	cur.mu.Unlock()
}

func newTxnQueue() *txnQueue {
	head := &txnQueueEntry{
		r: txnLockRequest{
			txnId:    math.MaxUint64, // Needed for the deadlock prevention policy
			lockMode: helper_ALLOW_ALL,
		},
	}
	tail := &txnQueueEntry{
		r: txnLockRequest{
			txnId:    0, // Needed for the deadlock prevention policy
			lockMode: helper_FORBID_ALL,
		},
	}
	head.next = tail
	tail.prev = head

	q := &txnQueue{
		head: head,
		tail: tail,

		mu:       sync.Mutex{},
		txnNodes: map[TransactionID]*txnQueueEntry{},
	}

	return q
}

func (q *txnQueue) Lock(r txnLockRequest) <-chan struct{} {
	q.head.mu.Lock()

	locksAreCompatible := true
	cur := q.head
	for {
		Assert(cur.r.txnId != r.txnId, "trying to lock already locked transaction. %v", r)
		locksAreCompatible = locksAreCompatible && compatibleLockModes(r.lockMode, cur.r.lockMode)

		if !locksAreCompatible && cur.r.txnId < r.txnId {
			// Deadlock prevention policy
			// Only an older transaction transactions can wait for a younger one.
			// Ohterwise, the younger transaction is aborted
			cur.mu.Unlock()
			return nil
		}

		if cur.next == q.tail {
			break
		}
		cur = cur.SafeNext()
	}
	defer cur.mu.Unlock()

	notifier := make(chan struct{})
	if locksAreCompatible {
		close(notifier) // Grant the lock immediately if the requested mode is compatible with the others
		newNode := &txnQueueEntry{
			r:        r,
			notifier: nil,
			isLocked: true,
		}
		cur.SafeInsert(newNode)

		q.mu.Lock()
		q.txnNodes[r.txnId] = newNode
		q.mu.Unlock()
		return notifier
	}
	newNode := &txnQueueEntry{
		r:        r,
		notifier: notifier,
		isLocked: false,
	}
	cur.SafeInsert(newNode)

	q.mu.Lock()
	q.txnNodes[r.txnId] = newNode
	q.mu.Unlock()
	return notifier
}

func (q *txnQueue) Unlock(r txnUnlockRequest) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	node, present := q.txnNodes[r.txnId]
	Assert(present, "node not found. %v", r)
	Assert(node.isLocked, "can't unlock an unlocked node. %v", r)

	delete(q.txnNodes, r.txnId)

	node.mu.Lock()
	defer node.mu.Unlock()

	prev := node.prev
	if !prev.mu.TryLock() {
		return false
	}

	next := node.next
	next.mu.Lock()

	prev.next = next
	next.prev = prev

	if prev == q.head && !next.isLocked {
		next.mu.Unlock()
		q.processBatch(prev)
		return true
	}

	next.mu.Unlock()
	prev.mu.Unlock()
	return true
}
