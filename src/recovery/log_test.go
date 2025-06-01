package recovery

import (
	"bytes"
	"testing"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	txns "github.com/Blackdeer1524/GraphDB/src/transactions"
	"github.com/stretchr/testify/require"
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
	slotNum, err := p.Insert([]byte("bef000"))
	if err != nil {
		t.Fatalf("couldn't insert a record: %v", err)
	}
	p.Unlock()
	pool.Unpin(pageID)

	txnId := txns.TxnID(100)
	before := []byte("before")
	after := []byte("after")

	chain := NewTxnLogChain(logger, txnId)
	// Simulate a transaction: Begin -> Insert -> Update -> Commit -> TxnEnd
	chain.Begin().
		Insert(pageID, slotNum, before).
		Update(pageID, slotNum, before, after).
		Commit().
		TxnEnd()
	err = chain.Err()
	if err != nil {
		t.Fatalf("log record append failed: %v", err)
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

func TestFailedTxn(t *testing.T) {
	pool := bufferpool.NewBufferPoolMock()

	logStart := LogRecordLocationInfo{
		Lsn:      0,
		Location: FileLocation{PageID: 0, SlotNum: 0},
	}
	logger := &TxnLogger{
		pool:            pool,
		logfileID:       1,
		lastLogLocation: logStart,
	}
	pageID := bufferpool.PageIdentity{FileID: 1, PageID: 42}

	p, err := pool.GetPage(pageID)
	require.Nil(t, err, "data page getting failed")
	p.Lock()
	slotNum, err := p.Insert([]byte("before"))
	require.Nil(t, err, "couldn't insert a record")
	p.Unlock()
	pool.Unpin(pageID)

	txnId := txns.TxnID(100)
	before := []byte("before")

	// Simulate a transaction: **Begin -> Insert -> CRASH**
	chain := NewTxnLogChain(logger, txnId).
		Begin().
		Insert(pageID, slotNum, before)
	require.Nil(t, chain.Err(), "log record append failed")

	// Simulate a crash and recovery
	logger2 := &TxnLogger{
		pool:            pool,
		logfileID:       1,
		lastLogLocation: logger.lastLogLocation,
	}
	checkpoint := LogRecordLocationInfo{Lsn: 0, Location: FileLocation{PageID: 0, SlotNum: 0}}
	logger2.Recover(checkpoint)

	iter, err := logger.Iter(logStart.Location)
	require.Nil(t, err, "couldn't create an iterator")

	succ, err := iter.MoveForward()
	if err != nil || !succ {
		t.Fatalf("couldn't move the iterator: %v", err)
	}

	//TODO
}
