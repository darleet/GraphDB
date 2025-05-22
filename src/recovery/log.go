package recovery

import (
	"errors"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/transactions"
)

type TxnLogger struct {
	pool bufferpool.BufferPool[*page.SlottedPage]

	// ================
	// лок на запись логов. Нужно для четкой упорядоченности
	// номеров записей и записей на диск
	mu sync.Mutex
	// "где-то" лежит на диске
	logRecordsCount uint64
	logfileID       uint64
	lastLogLocation LogRecordLocation
	// ================

	getActiveTransactions func() []transactions.TxnID // Прийдет из лок менеджера

}

func (l *TxnLogger) Iter(start PageLocation) (*LogRecordsIter, error) {
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

func (l *TxnLogger) Recover(checkpointLocation LogRecordLocation) {
	assert.Assert(checkpointLocation.isNil(), "the caller should have passed the first page of the log file")

	iter, err := l.Iter(PageLocation{
		PageID:  checkpointLocation.PageLoc.PageID,
		SlotNum: checkpointLocation.PageLoc.SlotNum,
	})
	assert.Assert(err == nil, "couldn't recover. reason: %+v", err)

	ATT, DPT := l.recoverAnalyze(iter)
	l.recoverRedo(ATT, DPT)
}

func (l *TxnLogger) recoverAnalyze(iter *LogRecordsIter) (ActiveTransactionsTable, map[bufferpool.PageIdentity]LogRecordLocation) {
	ATT := NewATT()
	DPT := map[bufferpool.PageIdentity]LogRecordLocation{}

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

			assert.Assert(ATT.Insert(
				record.txnId,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					LogRecordLocation{
						Lsn:     record.lsn,
						PageLoc: iter.Location(),
					}),
			), "Found a `begin` record for the already running transaction. TxnID: %d", record.txnId)
		case TypeInsert:
			record, ok := untypedRecord.(InsertLogRecord)
			assert.Assert(ok, "couldn't type cast the record")

			ATT.Insert(
				record.txnId,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					LogRecordLocation{
						Lsn:     record.lsn,
						PageLoc: iter.Location(),
					}),
			)

			_, alreadyExists := DPT[record.modifiedPageInfo]
			if !alreadyExists {
				DPT[record.modifiedPageInfo] = LogRecordLocation{
					Lsn:     record.lsn,
					PageLoc: iter.Location(),
				}
			}
		case TypeUpdate:
			record, ok := untypedRecord.(UpdateLogRecord)
			recordLocation := LogRecordLocation{
				Lsn:     record.lsn,
				PageLoc: iter.Location(),
			}
			assert.Assert(ok, "couldn't type cast the record")

			ATT.Insert(
				record.txnId,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					recordLocation,
				),
			)

			_, alreadyExists := DPT[record.modifiedPageInfo]
			if !alreadyExists {
				DPT[record.modifiedPageInfo] = recordLocation
			}
		case TypeCommit:
			record, ok := untypedRecord.(CommitLogRecord)
			assert.Assert(ok, "couldn't type cast the record")
			ATT.Insert(
				record.txnId,
				tag,
				NewATTEntry(
					TxnStatusCommit,
					LogRecordLocation{
						Lsn:     record.lsn,
						PageLoc: iter.Location(),
					}),
			)
		case TypeAbort:
			record, ok := untypedRecord.(AbortLogRecord)
			assert.Assert(ok, "couldn't type cast the record")

			ATT.Insert(
				record.txnId,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					LogRecordLocation{
						Lsn:     record.lsn,
						PageLoc: iter.Location(),
					}),
			)
		case TypeTxnEnd:
			record, ok := untypedRecord.(TxnEndLogRecord)
			assert.Assert(ok, "couldn't type cast the record")
			delete(ATT.table, record.txnId)
		case TypeCheckpointBegin:
		case TypeCheckpointEnd:
			record, ok := untypedRecord.(CheckpointEndLogRecord)
			assert.Assert(ok, "couldn't type cast the record")

			// Active Transactions Table
			// Monitors the last
			for _, txnId := range record.activeTransactions {
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
			DPT := map[bufferpool.PageIdentity]LogRecordLocation{}
			for pageInfo, firstDirtyLSN := range record.dirtyPageTable {
				if _, alreadyExists := DPT[pageInfo]; !alreadyExists {
					DPT[pageInfo] = firstDirtyLSN
				}
			}
		case TypeCompensation:
			record, ok := untypedRecord.(CompensationLogRecord)
			assert.Assert(ok, "couldn't type cast the record")

			ATT.Insert(
				record.txnId,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					LogRecordLocation{
						Lsn:     record.lsn,
						PageLoc: iter.Location(),
					}),
			)
		default:
			assert.Assert(tag < TypeUnknown, "unexpected log record type: %d", tag)
			panic("unreachable")
		}
	}

	return ATT, DPT
}

func (l *TxnLogger) recoverRedo(ATT ActiveTransactionsTable, DPT map[bufferpool.PageIdentity]LogRecordLocation) {
	for _, entry := range ATT.table {
		if entry.status != TxnStatusUndo {
			continue
		}
		recordLocation := entry.location.PageLoc
		clrsFound := 0
	outer:
		for {
			tag, record, err := l.readLogRecord(recordLocation)
			assert.Assert(err != nil, "todo")

			switch tag {
			case TypeBegin:
				assert.Assert(clrsFound == 0, "CLRs aren't balanced out")
				break outer
			case TypeInsert:
				record := record.(InsertLogRecord)
				DPT[record.modifiedPageInfo] = LogRecordLocation{
					Lsn:     record.lsn,
					PageLoc: recordLocation,
				}
				recordLocation = record.prevLogLocation.PageLoc
				if clrsFound > 0 {
					clrsFound--
				} else {
					l.undoInsert(&record, record.prevLogLocation)
				}
			case TypeUpdate:
				record := record.(UpdateLogRecord)
				DPT[record.modifiedPageInfo] = LogRecordLocation{
					Lsn:     record.lsn,
					PageLoc: recordLocation,
				}
				recordLocation = record.prevLogLocation.PageLoc
			case TypeCommit:
				assert.Assert(clrsFound == 0, "found CLRs for a commited txn")
				break outer
			case TypeAbort:
				record := record.(AbortLogRecord)
				recordLocation = record.prevLogLocation.PageLoc
			case TypeTxnEnd:
				assert.Assert(tag != TypeTxnEnd, "unreachable: ATT shouldn't have log records with TxnEnd logs")
			case TypeCompensation:
				clrsFound++
				record := record.(CompensationLogRecord)
				recordLocation = record.prevLogLocation.PageLoc
			case TypeCheckpointBegin:
				assert.Assert(tag != TypeCheckpointBegin, "unreachable: ATT shouldn't have CheckpointBegin records")
			case TypeCheckpointEnd:
				assert.Assert(tag != TypeCheckpointEnd, "unreachable: ATT shouldn't have CheckpointBegin records")
			default:
				assert.Assert(false, "unexpected record type: %d", tag)
				panic("unreachable")
			}
		}
	}

}

func (l *TxnLogger) readLogRecord(recordLocation PageLocation) (LogRecordTypeTag, any, error) {
	pageIdent := bufferpool.PageIdentity{
		FileID: l.logfileID,
		PageID: recordLocation.PageID,
	}
	page, err := l.pool.GetPage(pageIdent)
	if err != nil {
		return TypeUnknown, nil, err
	}
	defer l.pool.Unpin(pageIdent)

	page.RLock()
	record, err := page.Get(recordLocation.SlotNum)
	page.RUnlock()

	if err != nil {
		return TypeUnknown, nil, err
	}
	tag, r, err := readLogRecord(record)
	return tag, r, err
}

func (l *TxnLogger) writeLogRecord(serializedRecord []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	pageInfo := bufferpool.PageIdentity{
		FileID: l.logfileID,
		PageID: l.lastLogLocation.PageLoc.PageID,
	}

	p, err := l.pool.GetPage(pageInfo)
	if err != nil {
		return err
	}
	p.Lock()
	slotNumber, err := p.Insert(serializedRecord)
	p.Unlock()
	l.pool.Unpin(pageInfo)

	if errors.Is(err, page.ErrNoEnoughSpace) {
		l.lastLogLocation.PageLoc.PageID++

		pageInfo := bufferpool.PageIdentity{
			FileID: l.logfileID,
			PageID: l.lastLogLocation.PageLoc.PageID,
		}

		p, err = l.pool.GetPage(pageInfo)
		if err != nil {
			return err
		}
		p.Lock()
		slotNumber, err = p.Insert(serializedRecord)
		p.Unlock()
		l.pool.Unpin(pageInfo)

		assert.Assert(!errors.Is(err, page.ErrNoEnoughSpace), "very strange")
		if err != nil {
			return err
		}
		l.lastLogLocation.PageLoc.SlotNum = slotNumber
		return nil
	} else if err != nil {
		return err
	} else {
		l.lastLogLocation.PageLoc.SlotNum = slotNumber
		return nil
	}
}

func (l *TxnLogger) WriteBegin(txnId transactions.TxnID) (LSN, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewBeginLogRecord(lsn, txnId)
	bytes, err := r.MarshalBinary()
	if err != nil {
		return NIL_LSN, err
	}
	err = l.writeLogRecord(bytes)
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
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewUpdateLogRecord(lsn, txnId, prevLog, pageInfo, slotNumber, beforeValue, afterValue)
	bytes, err := r.MarshalBinary()
	if err != nil {
		return NIL_LSN, err
	}
	err = l.writeLogRecord(bytes)
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
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewInsertLogRecord(lsn, txnId, prevLog, pageInfo, slotNumber, value)
	bytes, err := r.MarshalBinary()
	if err != nil {
		return NIL_LSN, err
	}
	err = l.writeLogRecord(bytes)
	if err != nil {
		return NIL_LSN, err
	}
	return lsn, nil
}

func (l *TxnLogger) undoInsert(insertRecord *InsertLogRecord) {
}

func (l *TxnLogger) WriteCommit(
	txnId transactions.TxnID,
	prevLog LogRecordLocation,
) (LSN, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewCommitLogRecord(lsn, txnId, prevLog)
	bytes, err := r.MarshalBinary()
	if err != nil {
		return NIL_LSN, err
	}
	err = l.writeLogRecord(bytes)
	if err != nil {
		return NIL_LSN, err
	}
	return lsn, nil
}

func (l *TxnLogger) WriteAbort(
	txnId transactions.TxnID,
	prevLog LogRecordLocation,
) (LSN, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewAbortLogRecord(lsn, txnId, prevLog)
	bytes, err := r.MarshalBinary()
	if err != nil {
		return NIL_LSN, err
	}
	err = l.writeLogRecord(bytes)
	if err != nil {
		return NIL_LSN, err
	}
	return lsn, nil
}

func (l *TxnLogger) WriteTxnEnd(
	txnId transactions.TxnID,
	prevLog LogRecordLocation,
) (LSN, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewTxnEndLogRecord(lsn, txnId, prevLog)
	bytes, err := r.MarshalBinary()
	if err != nil {
		return NIL_LSN, err
	}
	err = l.writeLogRecord(bytes)
	if err != nil {
		return NIL_LSN, err
	}
	return lsn, nil
}

func (l *TxnLogger) WriteCheckpointBegin() (LSN, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewCheckpointBegin(lsn)
	bytes, err := r.MarshalBinary()
	if err != nil {
		return NIL_LSN, err
	}
	err = l.writeLogRecord(bytes)
	if err != nil {
		return NIL_LSN, err
	}
	return lsn, nil
}

func (l *TxnLogger) WriteCheckpointEnd(
	activeTransacitons []transactions.TxnID,
	dirtyPageTable map[bufferpool.PageIdentity]LogRecordLocation,
) (LSN, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewCheckpointEnd(lsn, activeTransacitons, dirtyPageTable)
	bytes, err := r.MarshalBinary()
	if err != nil {
		return NIL_LSN, err
	}
	err = l.writeLogRecord(bytes)
	if err != nil {
		return NIL_LSN, err
	}
	return lsn, nil
}
