package transactions

type RecordID uint64

type TransactionID uint64

type LockMode int

const (
	ALLOW_ALL LockMode = iota
	SHARED
	EXCLUSIVE
	FORBID_ALL
)

func compatibleLockModes(l LockMode, r LockMode) bool {
	if l == FORBID_ALL || r == FORBID_ALL {
		return false
	}
	if l == ALLOW_ALL || r == ALLOW_ALL {
		return true
	}
	if l == SHARED && r == SHARED {
		return true
	}
	return false
}

type txnLockRequest struct {
	/* a monotonically increasing counter.
	 * WARN: there might be problems with synchronization
	 * in distributed systems that use this kind of transaction IDs */
	txnId    TransactionID
	recordId RecordID
	lockMode LockMode
}

type txnUnlockRequest struct {
	txnId    TransactionID
	recordId RecordID
}
