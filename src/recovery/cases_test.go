package recovery

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type Integer interface {
	~int64 | ~uint64 | ~int
}

func generateUniqueInts[T Integer](t *testing.T, n, min, max int) []T {
	assert.LessOrEqual(t, min, max)

	nums := make(map[T]struct{}, n)
	res := make([]T, 0, n)
	for len(res) < n {
		for {
			val := T(rand.Intn(max-min+1) + min)
			if _, exists := nums[val]; !exists {
				nums[val] = struct{}{}
				res = append(res, val)
				break
			}
		}
	}
	return res
}

func TestBankTransactions(t *testing.T) {
	pool := bufferpool.NewBufferPoolMock()
	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinned()) }()

	q := generateUniqueInts[uint64](t, 2, 0, 1024)
	logger := &TxnLogger{
		pool:      pool,
		logfileID: q[0],
		lastLogLocation: LogRecordLocationInfo{
			Lsn:      0,
			Location: FileLocation{PageID: 0, SlotNum: 0},
		},
	}
	files := q[1:]

	BALANCE_LIMIT := uint32(200)

	recordValues := fillPages(
		t,
		logger,
		math.MaxUint64,
		100000,
		files,
		BALANCE_LIMIT,
	)
	IDs := make([]RecordID, len(recordValues))
	for i := range recordValues {
		IDs = append(IDs, i)
	}

	txnsCount := atomic.Uint64{}
	locker := txns.NewLocker()
	N := 10000

	wg := sync.WaitGroup{}
	for range N {
		wg.Add(1)
		go func() {
			defer wg.Done()
			txnID := txns.TxnID(txnsCount.Add(1))

			res := generateUniqueInts[int](t, 3, 0, len(IDs))
			me := IDs[res[0]]
			first := IDs[res[1]]
			second := IDs[res[2]]

			lastLogRecord, err := logger.AppendBegin(txnID)
			require.NoError(t, err)

			catalogLockOption := locker.LockCatalog(
				txnID,
				txns.GRANULAR_LOCK_SHARED,
			)
			require.True(t, catalogLockOption.IsSome())
			n, ctoken := catalogLockOption.Unwrap().Destruct()
			<-n
			defer locker.Unlock(ctoken)

			tableLockOpt := locker.LockTable(
				ctoken,
				txns.FileID(me.FileID),
				txns.GRANULAR_LOCK_INTENTION_SHARED,
			)
			if tableLockOpt.IsNone() {
				lastLogRecord, err = logger.AppendAbort(txnID, lastLogRecord)
				require.NoError(t, err)
				logger.Rollback(lastLogRecord)
				return
			}

			tableLock, ttoken := tableLockOpt.Unwrap().Destruct()
			<-tableLock

			myPageLockOpt := locker.LockPage(
				ttoken,
				txns.PageID(me.PageID),
				txns.PAGE_LOCK_SHARED,
			)
			if myPageLockOpt.IsNone() {
				lastLogRecord, err = logger.AppendAbort(txnID, lastLogRecord)
				require.NoError(t, err)
				logger.Rollback(lastLogRecord)
				return
			}
			<-myPageLockOpt.Unwrap()

			myPage, err := pool.GetPageNoCreate(me.PageIdentity())
			require.NoError(t, err)
			defer pool.Unpin(me.PageIdentity())
			myPage.RLock()
			myBalance := utils.BytesToUint32(myPage.Read(me.SlotNum))
			myPage.RUnlock()

			// try to read the first guy's balance
			firstPageLockOpt := locker.LockPage(
				ttoken,
				txns.PageID(first.PageID),
				txns.PAGE_LOCK_SHARED,
			)
			if firstPageLockOpt.IsNone() {
				lastLogRecord, err = logger.AppendAbort(txnID, lastLogRecord)
				require.NoError(t, err)
				logger.Rollback(lastLogRecord)
				return
			}
			<-firstPageLockOpt.Unwrap()

			firstPage, err := pool.GetPageNoCreate(first.PageIdentity())
			require.NoError(t, err)
			defer pool.Unpin(first.PageIdentity())

			firstPage.RLock()
			firstBalance := utils.BytesToUint32(firstPage.Read(first.SlotNum))
			firstPage.RUnlock()
			
			transferAmount := rand.Intn(int(myBalance)) + 1 

		}()
	}

	wg.Wait()

}
