package recovery

import (
	"errors"
	"math"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	txns "github.com/Blackdeer1524/GraphDB/src/transactions"
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
	lastLogLocation LogRecordLocationInfo
	// ================

	getActiveTransactions func() []txns.TxnID // Прийдет из лок менеджера

}

func (l *TxnLogger) Iter(start FileLocation) (*LogRecordsIter, error) {
	p, err := l.pool.GetPageNoCreate(bufferpool.PageIdentity{
		FileID: l.logfileID,
		PageID: start.PageID,
	})
	if err != nil {
		return nil, err
	}
	p.RLock()
	iter := newLogRecordIter(l.logfileID, start, l.pool, p)
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

func (l *TxnLogger) Recover(checkpointLocation LogRecordLocationInfo) {
	assert.Assert(checkpointLocation.isNil(), "the caller should have passed the first page of the log file")

	ATT, DPT := l.recoverAnalyze(checkpointLocation)
	earliestLog := l.recoverPrepareCLRs(ATT, DPT)
	l.recoverRedo(earliestLog.Location)
}

func (l *TxnLogger) recoverAnalyze(
	checkpointLocation LogRecordLocationInfo,
) (ActiveTransactionsTable, map[bufferpool.PageIdentity]LogRecordLocationInfo) {
	iter, err := l.Iter(FileLocation{
		PageID:  checkpointLocation.Location.PageID,
		SlotNum: checkpointLocation.Location.SlotNum,
	})
	assert.Assert(err == nil, "couldn't recover. reason: %+v", err)

	ATT := NewActiveTransactionsTable()
	DPT := map[bufferpool.PageIdentity]LogRecordLocationInfo{}

	for {
		tag, untypedRecord, err := iter.ReadRecord()
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
					LogRecordLocationInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
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
					LogRecordLocationInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
					}),
			)

			_, alreadyExists := DPT[record.modifiedPageIdentity]
			if !alreadyExists {
				DPT[record.modifiedPageIdentity] = LogRecordLocationInfo{
					Lsn:      record.lsn,
					Location: iter.Location(),
				}
			}
		case TypeUpdate:
			record, ok := untypedRecord.(UpdateLogRecord)
			recordLocation := LogRecordLocationInfo{
				Lsn:      record.lsn,
				Location: iter.Location(),
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

			_, alreadyExists := DPT[record.modifiedPageIdentity]
			if !alreadyExists {
				DPT[record.modifiedPageIdentity] = recordLocation
			}
		case TypeCommit:
			record, ok := untypedRecord.(CommitLogRecord)
			assert.Assert(ok, "couldn't type cast the record")
			ATT.Insert(
				record.txnId,
				tag,
				NewATTEntry(
					TxnStatusCommit,
					LogRecordLocationInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
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
					LogRecordLocationInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
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

			// Lecture #21: Database Crash Recovery @CMU
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
			// modified by uncommitted txns. There is one entry per dirty page
			// containing the recLSN (i.e., the LSN of the log record that first caused the page to be dirty).
			//
			// The DPT contains all pages that are dirty in the buffer pool.
			// It doesn’t matter if the changes were caused
			// by a transaction that is running, committed, or aborted.
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
					LogRecordLocationInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
					}),
			)
		default:
			assert.Assert(tag < TypeUnknown, "unexpected log record type: %d", tag)
			panic("unreachable")
		}

		success, err := iter.MoveForward()
		assert.Assert(err == nil, "%+v", err)
		if !success {
			break
		}
	}

	return ATT, DPT
}

func (l *TxnLogger) recoverPrepareCLRs(
	ATT ActiveTransactionsTable,
	DPT map[bufferpool.PageIdentity]LogRecordLocationInfo,
) LogRecordLocationInfo {
	earliestLogLocation := LogRecordLocationInfo{
		Lsn:      LSN(math.MaxUint64),
		Location: FileLocation{},
	}

	for _, entry := range ATT.table {
		if entry.status != TxnStatusUndo {
			continue
		}
		recordLocation := entry.logLocationInfo.Location
		clrsFound := 0
	outer:
		for {
			tag, record, err := l.readLogRecord(recordLocation)
			assert.Assert(err == nil, "todo")
			switch tag {
			case TypeBegin:
				record, ok := record.(BeginLogRecord)
				assert.Assert(ok, "todo")

				if earliestLogLocation.Lsn > record.lsn {
					earliestLogLocation.Lsn = record.lsn
					earliestLogLocation.Location = recordLocation
				}
				assert.Assert(clrsFound == 0, "CLRs aren't balanced out")

				_, err := l.AppendTxnEnd(record.txnId, entry.logLocationInfo)
				assert.Assert(err == nil, "todo")
				break outer
			case TypeInsert:
				record, ok := record.(InsertLogRecord)
				assert.Assert(ok, "todo")

				DPT[record.modifiedPageIdentity] = LogRecordLocationInfo{
					Lsn:      record.lsn,
					Location: recordLocation,
				}
				recordLocation = record.parentLogLocation.Location
				if clrsFound > 0 {
					clrsFound--
					continue
				}
				_, err := l.undoInsert(&record)
				assert.Assert(err == nil, "todo")
			case TypeUpdate:
				record, ok := record.(UpdateLogRecord)
				assert.Assert(ok, "todo")

				DPT[record.modifiedPageIdentity] = LogRecordLocationInfo{
					Lsn:      record.lsn,
					Location: recordLocation,
				}
				recordLocation = record.parentLogLocation.Location
				if clrsFound > 0 {
					clrsFound--
					continue
				}
				_, err := l.undoUpdate(&record)
				assert.Assert(err == nil, "todo")
			case TypeCommit:
				assert.Assert(clrsFound == 0, "found CLRs for a commited txn")
				break outer
			case TypeAbort:
				record := record.(AbortLogRecord)
				recordLocation = record.parentLogLocation.Location
			case TypeTxnEnd:
				assert.Assert(tag != TypeTxnEnd, "unreachable: ATT shouldn't have log records with TxnEnd logs")
			case TypeCompensation:
				clrsFound++
				record := record.(CompensationLogRecord)
				recordLocation = record.parentLogLocation.Location
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
	return earliestLogLocation
}

func (l *TxnLogger) recoverRedo(earliestLog FileLocation) {
	iter, err := l.Iter(earliestLog)
	assert.Assert(err == nil, "todo")

	for {
		tag, record, err := iter.ReadRecord()
		assert.Assert(err == nil, "todo")

		switch tag {
		case TypeInsert:
			record, ok := record.(InsertLogRecord)
			assert.Assert(ok, "todo")

			slotData, err := func() ([]byte, error) {
				p, err := l.pool.GetPage(record.modifiedPageIdentity)
				defer l.pool.Unpin(record.modifiedPageIdentity)
				assert.Assert(err == nil, "todo")

				p.Lock()
				defer p.Unlock()

				data, err := p.Get(record.modifiedSlotNumber)
				return data, err
			}()
			assert.Assert(!errors.Is(err, page.ErrInvalidSlotID), "(invariant) slot number should have been correct")
			assert.Assert(err == nil, "todo")
			assert.Assert(len(record.value) <= len(slotData), "new item len should be at most len of the old one")

			clear(slotData)
			copy(slotData, record.value)
		case TypeUpdate:
			record, ok := record.(UpdateLogRecord)
			assert.Assert(ok, "todo")

			slotData, err := func() ([]byte, error) {
				p, err := l.pool.GetPage(record.modifiedPageIdentity)
				defer l.pool.Unpin(record.modifiedPageIdentity)
				assert.Assert(err == nil, "todo")

				p.Lock()
				defer p.Unlock()

				data, err := p.Get(record.modifiedSlotNumber)
				return data, err
			}()
			assert.Assert(!errors.Is(err, page.ErrInvalidSlotID), "(invariant) slot number must be correct")
			assert.Assert(err == nil, "todo")
			assert.Assert(len(record.afterValue) <= len(slotData), "length should be the same")

			clear(slotData)
			copy(slotData, record.afterValue)
		case TypeCompensation:
			record, ok := record.(CompensationLogRecord)
			assert.Assert(ok, "todo")

			slotData, err := func() ([]byte, error) {
				p, err := l.pool.GetPage(record.modifiedPageIdentity)
				defer l.pool.Unpin(record.modifiedPageIdentity)
				assert.Assert(err == nil, "todo")

				p.Lock()
				defer p.Unlock()

				data, err := p.Get(record.modifiedSlotNumber)
				return data, err
			}()

			assert.Assert(!errors.Is(err, page.ErrInvalidSlotID), "(invariant) slot number must be correct")
			assert.Assert(err == nil, "todo")
			assert.Assert(len(record.afterValue) <= len(slotData), "length should be the same")

			clear(slotData)
			copy(slotData, record.afterValue)
		}

		success, err := iter.MoveForward()
		assert.Assert(err == nil, "todo")
		if !success {
			break
		}
	}
}

func (l *TxnLogger) readLogRecord(recordLocation FileLocation) (LogRecordTypeTag, any, error) {
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

func (lockedLogger *TxnLogger) writeLogRecord(serializedRecord []byte) (LogRecordLocationInfo, error) {
	pageInfo := bufferpool.PageIdentity{
		FileID: lockedLogger.logfileID,
		PageID: lockedLogger.lastLogLocation.Location.PageID,
	}

	p, err := lockedLogger.pool.GetPage(pageInfo)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	p.Lock()
	slotNumber, err := p.Insert(serializedRecord)
	p.Unlock()
	lockedLogger.pool.Unpin(pageInfo)

	if errors.Is(err, page.ErrNoEnoughSpace) {
		lockedLogger.lastLogLocation.Location.PageID++

		pageInfo := bufferpool.PageIdentity{
			FileID: lockedLogger.logfileID,
			PageID: lockedLogger.lastLogLocation.Location.PageID,
		}

		p, err = lockedLogger.pool.GetPage(pageInfo)
		if err != nil {
			return NewNilLogRecordLocation(), err
		}
		p.Lock()
		slotNumber, err = p.Insert(serializedRecord)
		p.Unlock()
		lockedLogger.pool.Unpin(pageInfo)

		assert.Assert(!errors.Is(err, page.ErrNoEnoughSpace), "very strange")
		if err != nil {
			return NewNilLogRecordLocation(), err
		}
		lockedLogger.lastLogLocation.Location.SlotNum = slotNumber
		return lockedLogger.lastLogLocation, nil
	} else if err != nil {
		return NewNilLogRecordLocation(), err
	} else {
		lockedLogger.lastLogLocation.Location.SlotNum = slotNumber
		return lockedLogger.lastLogLocation, nil
	}
}

func (l *TxnLogger) AppendBegin(txnId txns.TxnID) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewBeginLogRecord(lsn, txnId)

	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	return loc, nil
}

func (l *TxnLogger) AppendUpdate(
	txnId txns.TxnID,
	prevLog LogRecordLocationInfo,
	pageInfo bufferpool.PageIdentity,
	slotNumber uint32,
	beforeValue []byte,
	afterValue []byte,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewUpdateLogRecord(lsn, txnId, prevLog, pageInfo, slotNumber, beforeValue, afterValue)
	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	return loc, nil
}

func (l *TxnLogger) undoUpdate(updateRecord *UpdateLogRecord) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewCompensationLogRecord(
		lsn,
		updateRecord.txnId,
		l.lastLogLocation,
		updateRecord.modifiedPageIdentity,
		updateRecord.modifiedSlotNumber,
		false,
		updateRecord.parentLogLocation.Lsn,
		updateRecord.afterValue,
		updateRecord.beforeValue,
	)
	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	return loc, nil
}

func (l *TxnLogger) AppendInsert(
	txnId txns.TxnID,
	prevLog LogRecordLocationInfo,
	pageInfo bufferpool.PageIdentity,
	slotNumber uint32,
	value []byte,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewInsertLogRecord(lsn, txnId, prevLog, pageInfo, slotNumber, value)
	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	return loc, nil
}

func (l *TxnLogger) undoInsert(insertRecord *InsertLogRecord) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewCompensationLogRecord(
		lsn,
		insertRecord.txnId,
		l.lastLogLocation,
		insertRecord.modifiedPageIdentity,
		insertRecord.modifiedSlotNumber,
		true,
		insertRecord.parentLogLocation.Lsn,
		insertRecord.value,
		[]byte{},
	)
	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	return loc, nil
}

func (l *TxnLogger) AppendCommit(
	txnId txns.TxnID,
	prevLog LogRecordLocationInfo,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewCommitLogRecord(lsn, txnId, prevLog)
	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	return loc, nil
}

func (l *TxnLogger) AppendAbort(
	txnId txns.TxnID,
	prevLog LogRecordLocationInfo,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewAbortLogRecord(lsn, txnId, prevLog)
	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	return loc, nil
}

func (l *TxnLogger) AppendTxnEnd(
	txnId txns.TxnID,
	prevLog LogRecordLocationInfo,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewTxnEndLogRecord(lsn, txnId, prevLog)
	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	return loc, nil
}

func (l *TxnLogger) AppendCheckpointBegin() (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewCheckpointBegin(lsn)
	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	return loc, nil
}

func (l *TxnLogger) AppendCheckpointEnd(
	activeTransacitons []txns.TxnID,
	dirtyPageTable map[bufferpool.PageIdentity]LogRecordLocationInfo,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := LSN(l.logRecordsCount)
	l.logRecordsCount++

	r := NewCheckpointEnd(lsn, activeTransacitons, dirtyPageTable)
	bytes, err := r.MarshalBinary()
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	loc, err := l.writeLogRecord(bytes)
	if err != nil {
		return NewNilLogRecordLocation(), err
	}
	return loc, nil
}
