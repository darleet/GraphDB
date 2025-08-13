package txns

import (
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/optional"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
)

type PageID uint64
type FileID uint64

type Locker struct {
	catalogLockManager *Manager[GranularLockMode, struct{}]
	fileLockManager    *Manager[GranularLockMode, FileID] // for indexes and tables
	pageLockManager    *Manager[PageLockMode, common.PageIdentity]
}

func NewLocker() *Locker {
	return &Locker{
		catalogLockManager: NewManager[GranularLockMode, struct{}](),
		fileLockManager:    NewManager[GranularLockMode, FileID](),
		pageLockManager:    NewManager[PageLockMode, common.PageIdentity](),
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

type fileLockToken struct {
	txnID  TxnID
	fileID FileID
}

func newFileLockToken(txnID TxnID, fileID FileID) *fileLockToken {
	return &fileLockToken{
		fileID: fileID,
		txnID:  txnID,
	}
}

type pageLockToken struct {
	txnID  TxnID
	pageID common.PageIdentity
}

func newPageLockToken(
	txnID TxnID,
	pageID common.PageIdentity,
) *pageLockToken {
	return &pageLockToken{
		pageID: pageID,
		txnID:  txnID,
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

func (l *Locker) LockFile(
	t *catalogLockToken,
	fileID FileID,
	lockMode GranularLockMode,
) optional.Optional[utils.Pair[<-chan struct{}, *fileLockToken]] {
	n := l.fileLockManager.Lock(TxnLockRequest[GranularLockMode, FileID]{
		txnID:    t.txnID,
		objectId: fileID,
		lockMode: lockMode,
	})
	if n == nil {
		return optional.None[utils.Pair[<-chan struct{}, *fileLockToken]]()
	}

	tt := newFileLockToken(t.txnID, fileID)

	return optional.Some(
		utils.Pair[<-chan struct{}, *fileLockToken]{
			First:  n,
			Second: tt,
		},
	)
}

func (l *Locker) LockPage(
	t *fileLockToken,
	pageID PageID,
	lockMode PageLockMode,
) optional.Optional[utils.Pair[<-chan struct{}, *pageLockToken]] {
	pageIdent := common.PageIdentity{
		FileID: uint64(t.fileID),
		PageID: uint64(pageID),
	}

	lockRequest := TxnLockRequest[PageLockMode, common.PageIdentity]{
		txnID:    t.txnID,
		objectId: pageIdent,
		lockMode: lockMode,
	}

	n := l.pageLockManager.Lock(lockRequest)
	if n == nil {
		return optional.None[utils.Pair[<-chan struct{}, *pageLockToken]]()
	}

	return optional.Some(
		utils.Pair[<-chan struct{}, *pageLockToken]{
			First:  n,
			Second: newPageLockToken(t.txnID, pageIdent),
		},
	)
}

func (l *Locker) Unlock(t *catalogLockToken) {
	l.catalogLockManager.UnlockAll(t.txnID)
	l.fileLockManager.UnlockAll(t.txnID)
	l.pageLockManager.UnlockAll(t.txnID)
}

func (l *Locker) UpgradeCatalogLock(
	t *catalogLockToken,
	lockMode GranularLockMode,
) optional.Optional[<-chan struct{}] {
	n := l.catalogLockManager.Upgrade(
		TxnLockRequest[GranularLockMode, struct{}]{
			txnID:    t.txnID,
			objectId: struct{}{},
			lockMode: lockMode,
		},
	)

	if n == nil {
		return optional.None[<-chan struct{}]()
	}
	return optional.Some(n)
}

func (l *Locker) UpgradeFileLock(
	t *fileLockToken,
	lockMode GranularLockMode,
) optional.Optional[<-chan struct{}] {
	n := l.fileLockManager.Upgrade(TxnLockRequest[GranularLockMode, FileID]{
		txnID:    t.txnID,
		objectId: t.fileID,
		lockMode: lockMode,
	})
	if n == nil {
		return optional.None[<-chan struct{}]()
	}
	return optional.Some(n)
}

func (l *Locker) UpgradePageLock(
	t *pageLockToken,
	lockMode PageLockMode,
) optional.Optional[<-chan struct{}] {
	lockRequest := TxnLockRequest[PageLockMode, common.PageIdentity]{
		txnID:    t.txnID,
		objectId: t.pageID,
		lockMode: lockMode,
	}

	n := l.pageLockManager.Upgrade(lockRequest)
	if n == nil {
		return optional.None[<-chan struct{}]()
	}
	return optional.Some(n)
}
