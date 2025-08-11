package txns

import (
	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/optional"
)

type PageID uint64

type Locker struct {
	catalogLockManager Manager[GranularLockMode, struct{}]
	tableLockManager   Manager[GranularLockMode, TableID]
	pageLockManager    Manager[PageLockMode, bufferpool.PageIdentity]
}

type catalogLockToken struct {
	valid bool
	txnID TxnID

	child *tableLockToken
}

func newCatalogLockToken(txnID TxnID) *catalogLockToken {
	return &catalogLockToken{
		valid: true,
		child: nil,
		txnID: txnID,
	}
}

func (t *catalogLockToken) registerChild(tt *tableLockToken) {
	assert.Assert(t.isValid())
	assert.Assert(tt.isValid())

	tt.sibling = t.child
	t.child = tt
}

func (t *catalogLockToken) isValid() bool {
	return t.valid
}

func (t *catalogLockToken) invalidate() {
	t.valid = false
}

type tableLockToken struct {
	valid   bool
	txnID   TxnID
	tableID TableID

	sibling *tableLockToken
	child   *pageLockToken
}

func newTableLockToken(txnID TxnID, tableID TableID) *tableLockToken {
	return &tableLockToken{
		valid:   true,
		tableID: tableID,
		txnID:   txnID,
		sibling: nil,
		child:   nil,
	}
}

func (t *tableLockToken) registerChild(pt *pageLockToken) {
	assert.Assert(t.isValid())
	assert.Assert(pt.isValid())

	pt.sibling = t.child
	t.child = pt
}

func (t *tableLockToken) isValid() bool {
	return t.valid
}

func (t *tableLockToken) invalidate() {
	t.valid = false
}

type pageLockToken struct {
	valid  bool
	txnID  TxnID
	pageID bufferpool.PageIdentity

	sibling *pageLockToken
}

func newPageLockToken(
	txnID TxnID,
	pageID bufferpool.PageIdentity,
) *pageLockToken {
	return &pageLockToken{
		valid:   true,
		pageID:  pageID,
		txnID:   txnID,
		sibling: nil,
	}
}

func (t *pageLockToken) isValid() bool {
	return t.valid
}

func (t *pageLockToken) invalidate() {
	t.valid = false
}

type Pair[T, K any] struct {
	First  T
	Second K
}

func (l *Locker) LockCatalog(
	r TxnLockRequest[GranularLockMode, struct{}],
) optional.Optional[Pair[<-chan struct{}, *catalogLockToken]] {
	n := l.catalogLockManager.Lock(r)
	if n == nil {
		return optional.None[Pair[<-chan struct{}, *catalogLockToken]]()
	}

	return optional.Some(
		Pair[<-chan struct{}, *catalogLockToken]{
			n,
			newCatalogLockToken(r.txnID),
		},
	)
}

func (l *Locker) LockTable(
	t *catalogLockToken,
	r TxnLockRequest[GranularLockMode, TableID],
) optional.Optional[Pair[<-chan struct{}, *tableLockToken]] {
	assert.Assert(t.isValid())

	n := l.tableLockManager.Lock(r)
	if n == nil {
		return optional.None[Pair[<-chan struct{}, *tableLockToken]]()
	}

	tt := newTableLockToken(r.objectId)
	t.registerChild(tt)

	return optional.Some(
		Pair[<-chan struct{}, *tableLockToken]{
			n,
			tt,
		},
	)
}

func (l *Locker) LockPage(
	t *tableLockToken,
	r TxnLockRequest[PageLockMode, PageID],
) optional.Optional[Pair[<-chan struct{}, *pageLockToken]] {
	assert.Assert(t.isValid())

	pageID := bufferpool.PageIdentity{
		FileID: uint64(t.tableID),
		PageID: uint64(r.objectId),
	}

	lockRequest := TxnLockRequest[PageLockMode, bufferpool.PageIdentity]{
		txnID:    r.txnID,
		objectId: pageID,
		lockMode: r.lockMode,
	}

	n := l.pageLockManager.Lock(lockRequest)
	if n == nil {
		return optional.None[Pair[<-chan struct{}, *pageLockToken]]()
	}
	pt := newPageLockToken(r.txnID, pageID)
	t.registerChild(pt)

	return optional.Some(
		Pair[<-chan struct{}, *pageLockToken]{
			n,
			pt,
		},
	)
}

func (l *Locker) Unlock(t *catalogLockToken) {
	assert.Assert(t.isValid())

	unlockReq := TxnUnlockRequest[struct{}]{
		txnID:    t.txnID,
		objectId: struct{}{},
	}
	l.catalogLockManager.Unlock(unlockReq)

	for child := t.child; child != nil; child = child.sibling {
		assert.Assert(child.isValid())
		l.unlockTable(child)
	}
	t.invalidate()
}

func (l *Locker) unlockTable(t *tableLockToken) {
	assert.Assert(t.isValid())

	unlockReq := TxnUnlockRequest[TableID]{
		txnID:    t.txnID,
		objectId: t.tableID,
	}
	l.tableLockManager.Unlock(unlockReq)

	for child := t.child; child != nil; child = child.sibling {
		assert.Assert(child.isValid())
		l.unlockPage(child)
	}
	t.invalidate()
}

func (l *Locker) unlockPage(t *pageLockToken) {
	assert.Assert(t.isValid())

	unlockReq := TxnUnlockRequest[bufferpool.PageIdentity]{
		txnID:    t.txnID,
		objectId: t.pageID,
	}
	l.pageLockManager.Unlock(unlockReq)

	t.invalidate()
}
