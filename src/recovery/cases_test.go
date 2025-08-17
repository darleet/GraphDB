package recovery

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/panjf2000/ants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
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

	generatedFileIDs := generateUniqueInts[common.FileID](t, 2, 0, 1024)
	logger := &TxnLogger{
		pool:      pool,
		logfileID: generatedFileIDs[0],
		lastLogLocation: common.LogRecordLocInfo{
			Lsn:      0,
			Location: common.FileLocation{PageID: 0, SlotNum: 0},
		},
	}

	files := generatedFileIDs[1:]

	START_BALANCE := uint32(60)
	rollbackCutoff := START_BALANCE / 3
	clientsCount := 100
	txnsCount := 100_000

	workerPool, err := ants.NewPool(20_000)
	require.NoError(t, err)

	recordValues := fillPages(
		t,
		logger,
		math.MaxUint64,
		clientsCount,
		files,
		START_BALANCE,
	)
	require.NoError(t, pool.EnsureAllPagesUnpinned())

	txnsTicker := atomic.Uint64{}

	totalMoney := uint32(0)
	for id := range recordValues {
		page, err := pool.GetPageNoCreate(id.PageIdentity())
		require.NoError(t, err)
		page.Lock()
		page.Update(id.SlotNum, utils.Uint32ToBytes(START_BALANCE))
		totalMoney += START_BALANCE
		page.Unlock()
		assert.NoError(t, pool.Unpin(id.PageIdentity()))
	}

	IDs := []common.RecordID{}
	for i := range recordValues {
		IDs = append(IDs, i)
	}

	locker := txns.NewLocker()

	succ := atomic.Uint64{}
	wg := sync.WaitGroup{}
	task := func() {
		defer wg.Done()
		txnID := common.TxnID(txnsTicker.Add(1))
		logger := logger.WithContext(txnID)

		res := generateUniqueInts[int](t, 2, 0, len(IDs)-1)
		me := IDs[res[0]]
		first := IDs[res[1]]

		err := logger.AppendBegin()
		require.NoError(t, err)

		ctoken := locker.LockCatalog(
			txnID,
			txns.GRANULAR_LOCK_INTENTION_SHARED,
		)
		require.NotNil(t, ctoken)
		defer locker.Unlock(ctoken)

		ttoken := locker.LockFile(
			ctoken,
			common.FileID(me.FileID),
			txns.GRANULAR_LOCK_INTENTION_SHARED,
		)
		if ttoken == nil {
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return
		}

		myPageToken := locker.LockPage(
			ttoken,
			common.PageID(me.PageID),
			txns.PAGE_LOCK_SHARED,
		)
		if myPageToken == nil {
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return
		}

		myPage, err := pool.GetPageNoCreate(me.PageIdentity())
		require.NoError(t, err)
		defer func() { assert.NoError(t, pool.Unpin(me.PageIdentity())) }()

		myPage.RLock()
		myBalance := utils.BytesToUint32(myPage.Read(me.SlotNum))
		myPage.RUnlock()

		if myBalance == 0 {
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return
		}

		// try to read the first guy's balance
		firstPageToken := locker.LockPage(
			ttoken,
			common.PageID(first.PageID),
			txns.PAGE_LOCK_SHARED,
		)
		if firstPageToken == nil {
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return
		}

		firstPage, err := pool.GetPageNoCreate(first.PageIdentity())
		require.NoError(t, err)
		defer func() { assert.NoError(t, pool.Unpin(first.PageIdentity())) }()

		firstPage.RLock()
		firstBalance := utils.BytesToUint32(
			firstPage.Read(first.SlotNum),
		)
		firstPage.RUnlock()

		// transfering
		transferAmount := uint32(rand.Intn(int(myBalance)))
		if !locker.UpgradePageLock(myPageToken, txns.PAGE_LOCK_EXCLUSIVE) {
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return
		}

		if !locker.UpgradePageLock(firstPageToken, txns.PAGE_LOCK_EXCLUSIVE) {
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return
		}

		myPage.Lock()
		myNewBalance := utils.Uint32ToBytes(myBalance - transferAmount)
		myPage.UpdateWithLogs(myNewBalance, me, logger)
		myPage.Unlock()

		firstPage.Lock()
		firstNewBalance := utils.Uint32ToBytes(
			firstBalance + transferAmount,
		)
		firstPage.UpdateWithLogs(firstNewBalance, first, logger)
		firstPage.Unlock()

		myPage.RLock()
		myNewBalanceFromPage := utils.BytesToUint32(
			myPage.Read(me.SlotNum),
		)
		require.Equal(t, myNewBalanceFromPage, myBalance-transferAmount)
		myPage.RUnlock()

		firstPage.RLock()
		firstNewBalanceFromPage := utils.BytesToUint32(
			firstPage.Read(first.SlotNum),
		)
		require.Equal(
			t,
			firstNewBalanceFromPage,
			firstBalance+transferAmount,
		)
		firstPage.RUnlock()

		if myNewBalanceFromPage < rollbackCutoff {
			err = logger.AppendAbort()
			require.NoError(t, err)
			logger.Rollback()
			return
		}
		err = logger.AppendCommit()
		require.NoError(t, err)
		succ.Add(1)
	}

	for range txnsCount {
		wg.Add(1)
		require.NoError(t, workerPool.Submit(task))
	}
	wg.Wait()

	assert.Greater(t, txnsTicker.Load(), uint64(0))
	assert.Greater(t, succ.Load(), uint64(0))

	finalTotalMoney := uint32(0)
	for id := range recordValues {
		page, err := pool.GetPageNoCreate(id.PageIdentity())
		require.NoError(t, err)
		page.RLock()
		curMoney := utils.BytesToUint32(page.Read(id.SlotNum))
		finalTotalMoney += curMoney
		page.RUnlock()
		assert.NoError(t, pool.Unpin(id.PageIdentity()))
	}
	require.Equal(t, finalTotalMoney, totalMoney)
}
