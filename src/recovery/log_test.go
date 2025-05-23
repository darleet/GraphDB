package recovery

import (
	"bytes"
	"testing"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/transactions"
)

func TestValidRecovery(t *testing.T) {
	// Setup
	pool := bufferpool.NewBufferPoolMock()
	logger := &TxnLogger{
		pool:      pool,
		logfileID: 1,
		lastLogLocation: LogRecordLocationInfo{
			Lsn:      0,
			Location: FileLocation{PageID: 0, SlotNum: 0},
		},
	}

	pageID := bufferpool.PageIdentity{FileID: 1, PageID: 42}

	p, err := pool.GetPage(pageID)
	if err != nil {
		t.Fatalf("data page getting failed: %v", err)
	}
	p.Lock()
	slotNum, err := p.Insert([]byte("before"))
	if err != nil {
		t.Fatalf("couldn't insert a record: %v", err)
	}
	p.Unlock()
	pool.Unpin(pageID)

	txnId := transactions.TxnID(100)
	before := []byte("before")
	after := []byte("after")

	// Simulate a transaction: Begin -> Insert -> Update -> Commit -> TxnEnd
	lsnBegin, err := logger.AppendBegin(txnId)
	if err != nil {
		t.Fatalf("AppendBegin failed: %v", err)
	}

	lsnInsert, err := logger.AppendInsert(txnId, lsnBegin, pageID, slotNum, before)
	if err != nil {
		t.Fatalf("AppendInsert failed: %v", err)
	}

	lsnUpdate, err := logger.AppendUpdate(txnId, lsnInsert, pageID, slotNum, before, after)
	if err != nil {
		t.Fatalf("AppendUpdate failed: %v", err)
	}

	lsnCommit, err := logger.AppendCommit(txnId, lsnUpdate)
	if err != nil {
		t.Fatalf("AppendCommit failed: %v", err)
	}
	_, err = logger.AppendTxnEnd(txnId, lsnCommit)
	if err != nil {
		t.Fatalf("AppendTxnEnd failed: %v", err)
	}

	// Simulate a crash and recovery
	logger2 := &TxnLogger{
		pool:            pool,
		logfileID:       1,
		lastLogLocation: logger.lastLogLocation,
	}
	checkpoint := LogRecordLocationInfo{Lsn: 0, Location: FileLocation{PageID: 0, SlotNum: 0}}
	logger2.Recover(checkpoint)

	// Check that the page contains the "after" value
	p, err = pool.GetPage(pageID)
	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}
	data, err := p.Get(slotNum)
	if err != nil {
		t.Fatalf("Page.Get failed: %v", err)
	}
	if !bytes.Equal(data[:len(after)], after) {
		t.Errorf("Recovery failed: expected %q, got %q", after, data[:len(after)])
	}
}
