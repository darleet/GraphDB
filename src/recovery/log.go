package recovery

import (
	"encoding"
	"errors"
	"sync/atomic"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/transactions"
)

type TxnLogger struct {
	pool bufferpool.BufferPool[*page.SlottedPage]

	logRecordsCount atomic.Uint64 // "где-то" лежит на диске

	logfileID     uint64
	currentPageID atomic.Uint64

	getActiveTransactions func() []transactions.TxnID // Прийдет из лок менеджера

}

func (l *TxnLogger) Iter(start PageLocation) (*LogRecordIter, error) {
	p, err := l.pool.GetPageNoCreate(bufferpool.PageIdentity{
		FileID: l.logfileID,
		PageID: start.PageID,
	})
	if err != nil {
		return nil, err
	}
	p.RLock()
	iter := NewLogRecordIter(l.logfileID, start, l.pool, p)
	return iter, nil
}

/*
 * TODO: Разобраться где именн хранить
 * 1. точку начала (№ страницы лог файла) последнего чекпоинта
 *    Не обязательно сразу флашить на диск. Обязательно флашим
 *    точку оканчания чекпоинта <---- откуда восстанавливаться
 * 2. № страницы последней записи <---- куда начать писать
 *    после инициализации (флашить НЕ обязательно)
 */
func NewTxnLogger() {
	panic("not implemented")
}

func (l *TxnLogger) writeRecord(r encoding.BinaryMarshaler) error {
	bytes, err := r.MarshalBinary()
	if err != nil {
		return err
	}

	for {
		curPageID := l.currentPageID.Load()
		p, err := l.pool.GetPage(bufferpool.PageIdentity{
			FileID: l.logfileID,
			PageID: curPageID,
		})
		if err != nil {
			return err
		}

		p.Lock()
		_, err = p.Insert(bytes)
		p.Unlock()

		l.pool.Unpin(bufferpool.PageIdentity{FileID: l.logfileID, PageID: curPageID})
		if !errors.Is(err, page.ErrNoEnoughSpace) {
			break
		}
		l.currentPageID.CompareAndSwap(curPageID, curPageID+1)
	}
	if err != nil {
		return err
	}
	return nil

}

func (l *TxnLogger) WriteBegin(txnId transactions.TxnID) (LSN, error) {
	lsn := LSN(l.logRecordsCount.Add(1))
	r := BeginLogRecord{
		lsn:   lsn,
		txnId: txnId,
	}
	err := l.writeRecord(&r)
	if err != nil {
		return NIL_LSN, err
	}
	return lsn, nil
}

func (l *TxnLogger) WriteUpdate(
	txnId transactions.TxnID,
	prevLog LogRecordLocation,
	pageInfo bufferpool.PageIdentity,
	slotNumber uint32,
	beforeValue []byte,
	afterValue []byte) (LSN, error) {
	lsn := LSN(l.logRecordsCount.Add(1))
	r := NewUpdateLogRecord(lsn, txnId, prevLog, pageInfo, slotNumber, beforeValue, afterValue)
	err := l.writeRecord(&r)
	if err != nil {
		return NIL_LSN, err
	}
	return lsn, nil
}

func (l *TxnLogger) WriteInsert(
	txnId transactions.TxnID,
	prevLog LogRecordLocation,
	pageInfo bufferpool.PageIdentity,
	slotNumber uint32,
	value []byte) (LSN, error) {
	lsn := LSN(l.logRecordsCount.Add(1))
	r := NewInsertLogRecord(lsn, txnId, prevLog, pageInfo, slotNumber, value)
	err := l.writeRecord(&r)
	if err != nil {
		return NIL_LSN, err
	}
	return lsn, nil
}

func (l *TxnLogger) WriteCommit(
	txnId transactions.TxnID,
	prevLog LogRecordLocation,
) (LSN, error) {
	lsn := LSN(l.logRecordsCount.Add(1))
	r := NewCommitLogRecord(lsn, txnId, prevLog)
	err := l.writeRecord(&r)
	if err != nil {
		return NIL_LSN, err
	}
	return lsn, nil
}

func (l *TxnLogger) WriteAbort(
	txnId transactions.TxnID,
	prevLog LogRecordLocation,
) (LSN, error) {
	lsn := LSN(l.logRecordsCount.Add(1))
	r := NewAbortLogRecord(lsn, txnId, prevLog)
	err := l.writeRecord(&r)
	if err != nil {
		return NIL_LSN, err
	}
	return lsn, nil
}

func (l *TxnLogger) WriteTxnEnd(
	txnId transactions.TxnID,
	prevLog LogRecordLocation,
) (LSN, error) {
	lsn := LSN(l.logRecordsCount.Add(1))
	r := NewTxnEndLogRecord(lsn, txnId, prevLog)
	err := l.writeRecord(&r)
	if err != nil {
		return NIL_LSN, err
	}
	return lsn, nil
}

func (l *TxnLogger) WriteCLR(
	txnId transactions.TxnID,
	prevLog LogRecordLocation,
	pageInfo bufferpool.PageIdentity,
	slotNumber uint32,
	nextUndoLSN LSN,
	beforeValue []byte,
	afterValue []byte) (LSN, error) {
	lsn := LSN(l.logRecordsCount.Add(1))
	r := NewCompensationLogRecord(lsn, txnId, prevLog, pageInfo, slotNumber, nextUndoLSN, beforeValue, afterValue)
	err := l.writeRecord(&r)
	if err != nil {
		return NIL_LSN, err
	}
	return lsn, nil
}

func (l *TxnLogger) WriteCheckpointBegin() (LSN, error) {
	lsn := LSN(l.logRecordsCount.Add(1))
	r := NewCheckpointBegin(lsn)
	err := l.writeRecord(&r)
	if err != nil {
		return NIL_LSN, err
	}
	return lsn, nil
}

func (l *TxnLogger) WriteCheckpointEnd(
	activeTransacitons []transactions.TxnID,
	dirtyPageTable map[bufferpool.PageIdentity]LSN,
) (LSN, error) {
	lsn := LSN(l.logRecordsCount.Add(1))
	r := NewCheckpointEnd(lsn, activeTransacitons, dirtyPageTable)
	err := l.writeRecord(&r)
	if err != nil {
		return NIL_LSN, err
	}
	return lsn, nil
}

type txnStatus byte

const (
	TxnStatusUndo txnStatus = iota
	TxnStatusCommit
)

type ATTEntry struct {
	status   txnStatus
	location LogRecordLocation
}

func NewATTEntry(
	status txnStatus,
	location LogRecordLocation,
) ATTEntry {
	return ATTEntry{
		status:   status,
		location: location,
	}
}

type ActiveTransactionsTable struct {
	table map[transactions.TxnID]ATTEntry
}

func NewATT() ActiveTransactionsTable {
	return ActiveTransactionsTable{
		table: map[transactions.TxnID]ATTEntry{},
	}
}

// returns true iff it is the first record for the transaction
func (att *ActiveTransactionsTable) Insert(
	id transactions.TxnID,
	tag LogRecordTypeTag,
	entry ATTEntry,
) {
	if tag == TypeTxnEnd {
		delete(att.table, id)
		return
	}

	prevEntry, alreadyExists := att.table[id]
	if !alreadyExists {
		att.table[id] = entry
		return
	}
	// https://stackoverflow.com/questions/42605337/cannot-assign-to-struct-field-in-a-map
	if prevEntry.status == TxnStatusUndo {
		prevEntry.status = entry.status
	}
	prevEntry.location = entry.location
	att.table[id] = prevEntry
	return
}

func (l *TxnLogger) recoverAnalyze(
	iter *LogRecordIter,
	ATT ActiveTransactionsTable,
	DPT map[bufferpool.PageIdentity]LSN,
) {
	for {
		stop, err := iter.MoveForward()
		assert.Assert(err != nil, "%+v", err)
		if !stop {
			break
		}

		tag, untypedRecord, err := iter.Get()
		assert.Assert(err == nil, "couldn't read a record. reason: %+v", err)
		switch tag {
		case TypeBegin:
			record, ok := untypedRecord.(BeginLogRecord)
			assert.Assert(ok, "couldn't type cast the record")

			ATT.Insert(
				record.txnId,
				TypeBegin,
				NewATTEntry(
					TxnStatusUndo,
					LogRecordLocation{
						Lsn:     record.lsn,
						PageLoc: iter.Location(),
					}),
			)
		case TypeInsert:
			record, ok := untypedRecord.(InsertLogRecord)
			assert.Assert(ok, "couldn't type cast the record")

			ATT.Insert(
				record.txnId,
				TypeInsert,
				NewATTEntry(
					TxnStatusUndo,
					LogRecordLocation{
						Lsn:     record.lsn,
						PageLoc: iter.Location(),
					}),
			)

			_, alreadyExists := DPT[record.modifiedPageInfo]
			if !alreadyExists {
				DPT[record.modifiedPageInfo] = record.lsn
			}
		case TypeUpdate:
		case TypeCommit:
		case TypeAbort:
		case TypeTxnEnd:
		case TypeCheckpointBegin:
		case TypeCheckpointEnd:
		case TypeCompensation:
		default:
			assert.Assert(tag < TypeUnknown, "unexpected log record type: %d", tag)
			panic("unreachable")
		}
	}

}

func (l *TxnLogger) Recover(checkpointLocation LogRecordLocation) {
	assert.Assert(checkpointLocation.isNil(), "the caller should have passed the first page of the log file")

	iter, err := l.Iter(PageLocation{
		PageID:  checkpointLocation.PageLoc.PageID,
		SlotNum: checkpointLocation.PageLoc.SlotNum,
	})
	assert.Assert(err == nil, "couldn't recover. reason: %+v", err)

	// TODO: REDO THIS!!!
	// CheckpointBegin is Expected!!!
	tag, untypedRecord, err := iter.Get()
	panic("READ THE COMMENT ABOVE!!!")

	assert.Assert(err == nil, "couldn't recover. couldn't read a log record. reason: %+v", err)
	assert.Assert(tag == TypeCheckpointEnd, "expected a checkpoint record")

	checkpoint, ok := untypedRecord.(CheckpointEndLogRecord)
	// TODO: assertion will be triggered if there were no checkpoints at all
	assert.Assert(ok, "expected checkpoint end log record")

	// Active Transactions Table
	ATT := NewATT()
	for _, txnId := range checkpoint.activeTransactions {
		ATT.Insert(txnId, TypeBegin, NewATTEntry(
			TxnStatusUndo,
			NewNilLogRecordLocation(),
		))
	}

	// Dirty Page Table (DPT):
	// The DPT contains information about the pages in the buffer pool that were
	// modified by uncommitted transactions. There is one entry per dirty page
	// containing the recLSN (i.e., the LSN of the log record that first caused the page to be dirty).
	//
	// The DPT contains all pages that are dirty in the buffer pool.
	// It doesn’t matter if the changes were caused
	// by a transaction that is running, committed, or aborted.
	DPT := map[bufferpool.PageIdentity]LSN{}
	for pageInfo, firstDirtyLSN := range checkpoint.dirtyPageTable {
		DPT[pageInfo] = firstDirtyLSN
	}

}
