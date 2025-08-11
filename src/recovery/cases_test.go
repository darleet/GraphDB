package recovery

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
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

// func TestBankTransactions(t *testing.T) {
// 	pool := bufferpool.NewBufferPoolMock()
// 	defer func() { assert.NoError(t, pool.EnsureAllPagesUnpinned()) }()
//
// 	generatedFileIDs := generateUniqueInts[uint64](t, 2, 0, 1024)
// 	logger := &TxnLogger{
// 		pool:      pool,
// 		logfileID: generatedFileIDs[0],
// 		lastLogLocation: LogRecordLocationInfo{
// 			Lsn:      0,
// 			Location: FileLocation{PageID: 0, SlotNum: 0},
// 		},
// 	}
// 	files := generatedFileIDs[1:]
//
// 	BALANCE_LIMIT := uint32(200)
//
// 	recordValues := fillPages(
// 		t,
// 		logger,
// 		math.MaxUint64,
// 		100000,
// 		files,
// 		BALANCE_LIMIT,
// 	)
// 	totalMoney := uint32(0)
// 	for _, v := range recordValues {
// 		totalMoney += v
// 	}
//
// 	IDs := make([]RecordID, len(recordValues))
// 	for i := range recordValues {
// 		IDs = append(IDs, i)
// 	}
//
// 	txnsCount := atomic.Uint64{}
// 	locker := txns.NewLocker()
// 	N := 10000
//
// 	wg := sync.WaitGroup{}
// 	for range N {
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			txnID := txns.TxnID(txnsCount.Add(1))
//
// 			res := generateUniqueInts[int](t, 2, 0, len(IDs))
// 			me := IDs[res[0]]
// 			first := IDs[res[1]]
//
// 			lastLogRecord, err := logger.AppendBegin(txnID)
// 			require.NoError(t, err)
//
// 			catalogLockOption := locker.LockCatalog(
// 				txnID,
// 				txns.GRANULAR_LOCK_SHARED,
// 			)
// 			require.True(t, catalogLockOption.IsSome())
// 			n, ctoken := catalogLockOption.Unwrap().Destruct()
// 			<-n
// 			defer locker.Unlock(ctoken)
//
// 			tableLockOpt := locker.LockTable(
// 				ctoken,
// 				txns.FileID(me.FileID),
// 				txns.GRANULAR_LOCK_INTENTION_SHARED,
// 			)
// 			if tableLockOpt.IsNone() {
// 				lastLogRecord, err = logger.AppendAbort(txnID, lastLogRecord)
// 				require.NoError(t, err)
// 				logger.Rollback(lastLogRecord)
// 				return
// 			}
//
// 			tableLock, ttoken := tableLockOpt.Unwrap().Destruct()
// 			<-tableLock
//
// 			myPageLockOpt := locker.LockPage(
// 				ttoken,
// 				txns.PageID(me.PageID),
// 				txns.PAGE_LOCK_SHARED,
// 			)
// 			if myPageLockOpt.IsNone() {
// 				lastLogRecord, err = logger.AppendAbort(txnID, lastLogRecord)
// 				require.NoError(t, err)
// 				logger.Rollback(lastLogRecord)
// 				return
// 			}
// 			myPageLock, myPageToken := myPageLockOpt.Unwrap().Destruct()
// 			<-myPageLock
//
// 			myPage, err := pool.GetPageNoCreate(me.PageIdentity())
// 			require.NoError(t, err)
// 			defer pool.Unpin(me.PageIdentity())
// 			myPage.RLock()
// 			myBalance := utils.BytesToUint32(myPage.Read(me.SlotNum))
// 			myPage.RUnlock()
//
// 			// try to read the first guy's balance
// 			firstPageLockOpt := locker.LockPage(
// 				ttoken,
// 				txns.PageID(first.PageID),
// 				txns.PAGE_LOCK_SHARED,
// 			)
// 			if firstPageLockOpt.IsNone() {
// 				lastLogRecord, err = logger.AppendAbort(txnID, lastLogRecord)
// 				require.NoError(t, err)
// 				logger.Rollback(lastLogRecord)
// 				return
// 			}
// 			firstPageLock, firstPageToken := firstPageLockOpt.Unwrap().
// 				Destruct()
// 			<-firstPageLock
//
// 			firstPage, err := pool.GetPageNoCreate(first.PageIdentity())
// 			require.NoError(t, err)
// 			defer pool.Unpin(first.PageIdentity())
//
// 			firstPage.RLock()
// 			firstBalance := utils.BytesToUint32(firstPage.Read(first.SlotNum))
// 			firstPage.RUnlock()
//
// 			// transfering
// 			transferAmount := uint32(rand.Intn(int(myBalance)) + 1)
// 			myPageUpgradeLockOpt := locker.UpgradePageLock(
// 				myPageToken,
// 				txns.PAGE_LOCK_EXCLUSIVE,
// 			)
// 			if myPageUpgradeLockOpt.IsNone() {
// 				lastLogRecord, err = logger.AppendAbort(txnID, lastLogRecord)
// 				require.NoError(t, err)
// 				logger.Rollback(lastLogRecord)
// 				return
// 			}
// 			<-myPageUpgradeLockOpt.Unwrap()
//
// 			firstPageUpgradeLockOpt := locker.UpgradePageLock(
// 				firstPageToken,
// 				txns.PAGE_LOCK_EXCLUSIVE,
// 			)
// 			if firstPageUpgradeLockOpt.IsNone() {
// 				lastLogRecord, err = logger.AppendAbort(txnID, lastLogRecord)
// 				require.NoError(t, err)
// 				logger.Rollback(lastLogRecord)
// 				return
// 			}
// 			<-firstPageUpgradeLockOpt.Unwrap()
//
// 			myPage.Lock()
// 			myNewBalance := utils.Uint32ToBytes(myBalance - transferAmount)
// 			myPage.Update(me.SlotNum, myNewBalance)
// 			myPage.Unlock()
//
// 			firstPage.Lock()
// 			firstNewBalance := utils.Uint32ToBytes(
// 				firstBalance + transferAmount,
// 			)
// 			firstPage.Update(first.SlotNum, firstNewBalance)
// 			firstPage.Unlock()
//
// 			myPage.RLock()
// 			myNewBalanceFromPage := utils.BytesToUint32(myPage.Read(me.SlotNum))
// 			require.Equal(t, myNewBalanceFromPage, myNewBalance)
// 			myPage.RUnlock()
//
// 			firstPage.RLock()
// 			firstNewBalanceFromPage := utils.BytesToUint32(
// 				firstPage.Read(first.SlotNum),
// 			)
// 			require.Equal(t, firstNewBalanceFromPage, firstNewBalance)
// 			firstPage.RUnlock()
//
// 			if myNewBalanceFromPage <= 30 {
// 				lastLogRecord, err = logger.AppendAbort(txnID, lastLogRecord)
// 				require.NoError(t, err)
// 				logger.Rollback(lastLogRecord)
// 				return
// 			}
// 			lastLogRecord, err = logger.AppendCommit(txnID, lastLogRecord)
// 			require.NoError(t, err)
// 		}()
// 	}
// 	wg.Wait()
//
// 	finalTotalMoney := uint32(0)
// 	for id := range recordValues {
// 		page, err := pool.GetPageNoCreate(id.PageIdentity())
// 		require.NoError(t, err)
//
// 		page.RLock()
// 		finalTotalMoney += utils.BytesToUint32(page.Read(id.SlotNum))
// 		page.RUnlock()
// 	}
// 	require.Equal(t, finalTotalMoney, totalMoney)
// }
