package txns

import (
	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/optional"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
)

type PageID uint64
type FileID uint64

type Locker struct {
	catalogLockManager *Manager[GranularLockMode, struct{}]
	fileLockManager    *Manager[GranularLockMode, FileID] // for indexes and tables
	pageLockManager    *Manager[PageLockMode, bufferpool.PageIdentity]
}

func NewLocker() *Locker {
	return &Locker{
		catalogLockManager: NewManager[GranularLockMode, struct{}](),
		fileLockManager:    NewManager[GranularLockMode, FileID](),
		pageLockManager:    NewManager[PageLockMode, bufferpool.PageIdentity](),
	}
}

type catalogLockToken struct {
	txnID TxnID
}

func newCatalogLockToken(txnID TxnID) *catalogLockToken {
	return &catalogLockToken{
		txnID: txnID,
	}
}

type tableLockToken struct {
	txnID   TxnID
	tableID FileID
}

func newTableLockToken(txnID TxnID, tableID FileID) *tableLockToken {
	return &tableLockToken{
		tableID: tableID,
		txnID:   txnID,
	}
}

func (l *Locker) LockCatalog(
	txnID TxnID,
	lockMode GranularLockMode,
) optional.Optional[utils.Pair[<-chan struct{}, *catalogLockToken]] {
	r := TxnLockRequest[GranularLockMode, struct{}]{
		txnID:    txnID,
		objectId: struct{}{},
		lockMode: lockMode,
	}

	n := l.catalogLockManager.Lock(r)
	if n == nil {
		return optional.None[utils.Pair[<-chan struct{}, *catalogLockToken]]()
	}

	return optional.Some(
		utils.Pair[<-chan struct{}, *catalogLockToken]{
			First:  n,
			Second: newCatalogLockToken(r.txnID),
		},
	)
}

func (l *Locker) LockTable(
	t *catalogLockToken,
	tableID FileID,
	lockMode GranularLockMode,
) optional.Optional[utils.Pair[<-chan struct{}, *tableLockToken]] {
	n := l.fileLockManager.Lock(TxnLockRequest[GranularLockMode, FileID]{
		txnID:    t.txnID,
		objectId: tableID,
		lockMode: lockMode,
	})
	if n == nil {
		return optional.None[utils.Pair[<-chan struct{}, *tableLockToken]]()
	}

	tt := newTableLockToken(t.txnID, tableID)

	return optional.Some(
		utils.Pair[<-chan struct{}, *tableLockToken]{
			First:  n,
			Second: tt,
		},
	)
}

func (l *Locker) LockPage(
	t *tableLockToken,
	pageID PageID,
	lockMode PageLockMode,
) optional.Optional[<-chan struct{}] {
	pageIdent := bufferpool.PageIdentity{
		FileID: uint64(t.tableID),
		PageID: uint64(pageID),
	}

	lockRequest := TxnLockRequest[PageLockMode, bufferpool.PageIdentity]{
		txnID:    t.txnID,
		objectId: pageIdent,
		lockMode: lockMode,
	}

	n := l.pageLockManager.Lock(lockRequest)
	if n == nil {
		return optional.None[<-chan struct{}]()
	}
	return optional.Some(n)
}

func (l *Locker) Unlock(t *catalogLockToken) {
	l.catalogLockManager.UnlockAll(t.txnID)
	l.fileLockManager.UnlockAll(t.txnID)
	l.pageLockManager.UnlockAll(t.txnID)
}
