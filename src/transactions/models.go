package transactions

type RecordID uint64

/* a monotonically increasing counter. It is guaranteed to be unique between transactions
 * WARN: there might be problems with synchronization
 *       in distributed systems that use this kind of transaction IDs */
type TxnID uint64

type TaggedLockMode[TypeTag any] int

type RecordLockMode TaggedLockMode[struct{}]
type TableLockMode TaggedLockMode[struct{}]

type LockMode[T any] interface {
	Compatible(T) bool
}

const (
	RECORD_LOCK_SHARED RecordLockMode = iota
	RECORD_LOCK_EXCLUSIVE
)

const (
	TABLE_LOCK_SHARED TableLockMode = iota
	TABLE_LOCK_EXCLUSIVE
	TABLE_LOCK_INTENTION_EXCLUSIVE
	TABLE_LOCK_INTENTION_SHARED
	TABLE_LOCK_SHARED_INTENTION_EXCLUSIVE
)

func (m RecordLockMode) Compatible(other RecordLockMode) bool {
	if m == RECORD_LOCK_SHARED && other == RECORD_LOCK_SHARED {
		return true
	}
	return false
}

func (m TableLockMode) Compatible(other TableLockMode) bool {
	panic("NOT IMPLEMENTED")
}

type TxnLockRequest[LockModeType LockMode[LockModeType]] struct {
	txnID    TxnID
	recordId RecordID
	lockMode LockModeType
}

func NewTxnLockRequest[LockModeType LockMode[LockModeType]](txnID TxnID, recordId RecordID, lockMode LockModeType) *TxnLockRequest[LockModeType] {
	return &TxnLockRequest[LockModeType]{
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
