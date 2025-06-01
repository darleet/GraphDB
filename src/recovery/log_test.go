package recovery

import (
	"bytes"
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	txns "github.com/Blackdeer1524/GraphDB/src/transactions"
	"github.com/stretchr/testify/require"
)

func TestValidRecovery(t *testing.T) {
	pool := bufferpool.NewBufferPoolMock()
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinned()) }()

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
	checkpoint := FileLocation{PageID: 0, SlotNum: 0}
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

	txnId := txns.TxnID(100)
	before := []byte("before")

	p, err := pool.GetPage(pageID)
	require.NoError(t, err, "data page getting failed")
	p.Lock()
	slotNum, err := p.Insert(before)
	require.NoError(t, err, "couldn't insert a record")
	p.Unlock()
	pool.Unpin(pageID)

	// Simulate a transaction: **Begin -> Insert -> CRASH**
	chain := NewTxnLogChain(logger, txnId).
		Begin().
		Insert(pageID, slotNum, before)
	require.Nil(t, chain.Err(), "log record append failed")

	// Simulate a crash and recovery
	checkpoint := logStart.Location
	logger.Recover(checkpoint)

	// BEGIN
	iter, err := logger.Iter(logStart.Location)
	require.NoError(t, err, "couldn't create an iterator")
	{
		tag, untypedRecord, err := iter.ReadRecord()
		require.NoError(t, err)
		assertLogRecord(t, tag, untypedRecord, TypeBegin, txnId)
	}

	// INSERT
	ok, err := iter.MoveForward()
	require.NoError(t, err, "couldn't move the iterator")
	require.True(t, ok)
	{
		tag, untypedRecord, err := iter.ReadRecord()
		require.NoError(t, err)
		assertLogRecord(t, tag, untypedRecord, TypeInsert, txnId)
	}

	// CLR
	ok, err = iter.MoveForward()
	require.NoError(t, err, "couldn't move the iterator")
	require.True(t, ok)
	{
		tag, untypedRecord, err := iter.ReadRecord()
		require.NoError(t, err)
		assertLogRecord(t, tag, untypedRecord, TypeCompensation, txnId)
	}

	// TxnEnd
	ok, err = iter.MoveForward()
	require.NoError(t, err, "couldn't move the iterator")
	require.True(t, ok)
	{
		tag, untypedRecord, err := iter.ReadRecord()
		require.NoError(t, err)
		assertLogRecord(t, tag, untypedRecord, TypeTxnEnd, txnId)
	}

	// NOTHING
	ok, err = iter.MoveForward()
	require.NoError(t, err, "couldn't move the iterator")
	require.False(t, ok)
}

func TestMassiveRecovery(t *testing.T) {
	pool := bufferpool.NewBufferPoolMock()
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinned()) }()

	logPageId := bufferpool.PageIdentity{
		FileID: 42,
		PageID: 321,
	}

	logger := &TxnLogger{
		pool:            pool,
		mu:              sync.Mutex{},
		logRecordsCount: 0,
		logfileID:       logPageId.FileID,
		lastLogLocation: LogRecordLocationInfo{
			Lsn: 0,
			Location: FileLocation{
				PageID:  logPageId.PageID,
				SlotNum: 0,
			},
		},
		getActiveTransactions: func() []txns.TxnID {
			panic("TODO")
		},
	}

	INIT := []byte("init")
	NEW := []byte("new")
	NEW2 := []byte("123")

	ORIGIN := bufferpool.PageIdentity{
		FileID: 0,
		PageID: 0,
	}
	dataPageId := ORIGIN
	slot := uint32(0)

	N := 1000
	i := 0

	index2pageID := map[int]FileLocation{}
	for i < N {
		succ := func() bool {
			p, err := pool.GetPage(dataPageId)
			require.NoError(t, err)
			defer pool.Unpin(dataPageId)

			p.Lock()
			defer p.Unlock()

			slot, err = p.Insert(INIT)
			if errors.Is(err, page.ErrNoEnoughSpace) {
				dataPageId.PageID++
				return false
			}
			require.NoError(t, err)

			return true
		}()
		if succ {
			index2pageID[i] = FileLocation{
				PageID:  dataPageId.PageID,
				SlotNum: slot,
			}
			i++
		}
	}

	txnIdCounter := atomic.Uint64{}

	left := rand.Int() % N
	inc := N * 6 / 10
	right := (left + inc) % N
	STEP := 5

	assert.Assert(inc%STEP == 0, "step must divide inc. otherwise, it would cause an infinite loop")
	wg := sync.WaitGroup{}
	for i := left; i != right; i = (i + STEP) % N {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			txnId := txns.TxnID(txnIdCounter.Add(1))
			chain := NewTxnLogChain(logger, txnId)

			chain.Begin()
			for j := range STEP {
				recordLoc := index2pageID[i+j]
				chain.
					Update(bufferpool.PageIdentity{
						FileID: 0,
						PageID: recordLoc.PageID,
					}, recordLoc.SlotNum, INIT, NEW).
					Update(bufferpool.PageIdentity{
						FileID: 0,
						PageID: recordLoc.PageID,
					}, recordLoc.SlotNum, NEW, NEW2)
			}
			require.NoError(t, chain.Err())
		}(i)
	}
	wg.Wait()

	logger.Recover(FileLocation{
		PageID:  logPageId.PageID,
		SlotNum: 0,
	})

	for i := range N {
		func() {
			location := index2pageID[i]
			dataPageId := bufferpool.PageIdentity{
				FileID: 0,
				PageID: location.PageID,
			}
			p, err := pool.GetPageNoCreate(dataPageId)
			require.NoError(t, err)
			defer pool.Unpin(dataPageId)

			p.RLock()
			defer p.RUnlock()

			data, err := p.Get(location.SlotNum)
			require.NoError(t, err)

			require.Equal(t, len(INIT), len(data))
			for i := range len(INIT) {
				require.Equal(t, INIT[i], data[i])
			}
		}()
	}
}

func assertLogRecord(
	t *testing.T,
	actualTag LogRecordTypeTag,
	untypedRecord any,
	expectedRecordType LogRecordTypeTag,
	expectedTxnID txns.TxnID,
) {
	require.Equal(t, actualTag, expectedRecordType)
	switch actualTag {
	case TypeBegin:
		r, ok := untypedRecord.(BeginLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTxnID, r.txnId)
	case TypeUpdate:
		r, ok := untypedRecord.(UpdateLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTxnID, r.txnId)
	case TypeInsert:
		r, ok := untypedRecord.(InsertLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTxnID, r.txnId)
	case TypeCommit:
		r, ok := untypedRecord.(CommitLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTxnID, r.txnId)
	case TypeAbort:
		r, ok := untypedRecord.(AbortLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTxnID, r.txnId)
	case TypeTxnEnd:
		r, ok := untypedRecord.(TxnEndLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTxnID, r.txnId)
	case TypeCompensation:
		r, ok := untypedRecord.(CompensationLogRecord)
		require.True(t, ok)
		require.Equal(t, expectedTxnID, r.txnId)
	default:
		require.Less(t, actualTag, TypeUnknown)
	}

}

func assertLogRecordWithRetrieval(
	t *testing.T,
	pool bufferpool.BufferPool[*page.SlottedPage],
	pageID bufferpool.PageIdentity,
	slotNum uint32,
	expectedRecordType LogRecordTypeTag,
	expectedTxnID txns.TxnID,
) {
	page, err := pool.GetPage(pageID)
	require.NoError(t, err)
	page.RLock()

	data, err := page.Get(slotNum)
	require.NoError(t, err)

	tag, untypedRecord, err := readLogRecord(data)
	require.NoError(t, err)

	assertLogRecord(t, tag, untypedRecord, expectedRecordType, expectedTxnID)

	page.RUnlock()
	pool.Unpin(pageID)
}

func TestLoggerValidConcurrentWrites(t *testing.T) {
	pool := bufferpool.NewBufferPoolMock()
	defer func() { require.NoError(t, pool.EnsureAllPagesUnpinned()) }()

	logPageId := bufferpool.PageIdentity{
		FileID: 42,
		PageID: 321,
	}

	logger := &TxnLogger{
		pool:            pool,
		mu:              sync.Mutex{},
		logRecordsCount: 0,
		logfileID:       logPageId.FileID,
		lastLogLocation: LogRecordLocationInfo{
			Lsn: 0,
			Location: FileLocation{
				PageID:  logPageId.PageID,
				SlotNum: 0,
			},
		},
		getActiveTransactions: func() []txns.TxnID {
			panic("TODO")
		},
	}

	dataPageId := bufferpool.PageIdentity{
		FileID: 0,
		PageID: 0,
	}

	waitWg := sync.WaitGroup{}
	barierWg := sync.WaitGroup{}

	OUTER := 100
	INNER := 10

	for i := range OUTER {
		waitWg.Add(1)
		barierWg.Add(1)
		go func(txnID txns.TxnID) {
			defer waitWg.Done()

			chain := NewTxnLogChain(logger, txnID)

			insertLocs := []LogRecordLocationInfo{}
			updateLocs := []LogRecordLocationInfo{}
			beginLoc := chain.Begin().Loc()
			for j := range INNER {
				switch rand.Int() % 2 {
				case 0:
					insertLocs = append(
						insertLocs,
						chain.Insert(dataPageId, uint32(j), []byte(strconv.Itoa(i*INNER+j))).Loc(),
					)
				case 1:
					updateLocs = append(
						updateLocs,
						chain.Update(dataPageId, uint32(j), []byte(strconv.Itoa(i)), []byte(strconv.Itoa(i*INNER+j))).Loc(),
					)
				}
			}
			finishLoc := NewNilLogRecordLocation()
			isCommit := rand.Int()%2 != 0
			if isCommit {
				finishLoc = chain.Commit().Loc()
			} else {
				finishLoc = chain.Abort().Loc()
			}
			chain.TxnEnd()
			require.NoError(t, chain.Err())

			barierWg.Done()
			barierWg.Wait()

			assertLogRecordWithRetrieval(t, logger.pool, bufferpool.PageIdentity{FileID: logger.logfileID, PageID: beginLoc.Location.PageID}, beginLoc.Location.SlotNum, TypeBegin, txnID)
			if isCommit {
				assertLogRecordWithRetrieval(t, logger.pool, bufferpool.PageIdentity{FileID: logger.logfileID, PageID: finishLoc.Location.PageID}, finishLoc.Location.SlotNum, TypeCommit, txnID)
			} else {
				assertLogRecordWithRetrieval(t, logger.pool, bufferpool.PageIdentity{FileID: logger.logfileID, PageID: finishLoc.Location.PageID}, finishLoc.Location.SlotNum, TypeAbort, txnID)
			}

			for _, insert := range insertLocs {
				assertLogRecordWithRetrieval(t, logger.pool, bufferpool.PageIdentity{
					FileID: logger.logfileID,
					PageID: insert.Location.PageID,
				}, insert.Location.SlotNum, TypeInsert, txnID)
			}

			for _, update := range updateLocs {
				assertLogRecordWithRetrieval(t, logger.pool, bufferpool.PageIdentity{
					FileID: logger.logfileID,
					PageID: update.Location.PageID,
				}, update.Location.SlotNum, TypeUpdate, txnID)
			}
		}(txns.TxnID(i))
	}

	waitWg.Wait()
}
