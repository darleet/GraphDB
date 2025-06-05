package transactions

type RecordID uint64

/* a monotonically increasing counter. It is guaranteed to be unique between transactions
 * WARN: there might be problems with synchronization
 *       in distributed systems that use this kind of transaction IDs */
type TxnID uint64

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

type TxnLockRequest struct {
	txnID    TxnID
	recordId RecordID
	lockMode LockMode
}

func NewTxnLockRequest(txnID TxnID, recordId RecordID, lockMode LockMode) *TxnLockRequest {
	return &TxnLockRequest{
		txnID:    txnID,
		recordId: recordId,
		lockMode: lockMode,
	}
}

type TxnUnlockRequest struct {
	txnID    TxnID
	recordId RecordID
}

func NewTxnUnlockRequest(txnID TxnID, recordId RecordID) *TxnUnlockRequest {
	return &TxnUnlockRequest{
		txnID:    txnID,
		recordId: recordId,
	}
}
