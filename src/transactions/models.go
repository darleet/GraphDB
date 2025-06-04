package transactions

type RecordID uint64

/* a monotonically increasing counter. It is guaranteed to be unique between transactions
 * WARN: there might be problems with synchronization
 *       in distributed systems that use this kind of transaction IDs */
type TransactionID uint64

type LockMode int

const (
	helper_ALLOW_ALL LockMode = iota
	SHARED
	EXCLUSIVE
	helper_FORBID_ALL
)

func compatibleLockModes(l LockMode, r LockMode) bool {
	if l == helper_FORBID_ALL || r == helper_FORBID_ALL {
		return false
	}
	if l == helper_ALLOW_ALL || r == helper_ALLOW_ALL {
		return true
	}
	if l == SHARED && r == SHARED {
		return true
	}
	return false
}

type txnLockRequest struct {
	TransactionID    TransactionID
	recordId RecordID
	lockMode LockMode
}

type txnUnlockRequest struct {
	TransactionID    TransactionID
	recordId RecordID
}
