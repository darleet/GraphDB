package recovery

import (
	"errors"
	"math"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/txns"
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

func (l *TxnLogger) iter(
	start FileLocation,
) (*LogRecordsIter, error) {
	p, err := l.pool.GetPageNoCreate(bufferpool.PageIdentity{
		FileID: l.logfileID,
		PageID: start.PageID,
	})
	if err != nil {
		return nil, err
	}

	p.RLock()
	iter := newLogRecordIter(
		l.logfileID,
		start,
		l.pool,
		p,
	)

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

func (l *TxnLogger) Recover(checkpointLocation FileLocation) {
	ATT, DPT := l.recoverAnalyze(checkpointLocation)
	earliestLog := l.recoverPrepareCLRs(ATT, DPT)
	l.recoverRedo(earliestLog.Location)
}

func (l *TxnLogger) recoverAnalyze(
	checkpointLocation FileLocation,
) (ActiveTransactionsTable, map[bufferpool.PageIdentity]LogRecordLocationInfo) {
	iter, err := l.iter(FileLocation{
		PageID:  checkpointLocation.PageID,
		SlotNum: checkpointLocation.SlotNum,
	})
	assert.Assert(err == nil, "couldn't recover. reason: %+v", err)

	ATT := NewActiveTransactionsTable()
	DPT := map[bufferpool.PageIdentity]LogRecordLocationInfo{}

	for {
		tag, untypedRecord, err := iter.ReadRecord()
		assert.Assert(err == nil, "couldn't read a record. reason: %+v", err)

		switch tag {
		case TypeBegin:
			record := assert.Cast[BeginLogRecord](untypedRecord)
			assert.Assert(ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					LogRecordLocationInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
					}),
			), "Found a `begin` record for the already running transaction. TransactionID: %d", record.txnID)
		case TypeInsert:
			record := assert.Cast[InsertLogRecord](untypedRecord)

			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					LogRecordLocationInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
					}),
			)

			pageID := record.modifiedRecordID.PageIdentity()
			_, alreadyExists := DPT[pageID]
			if !alreadyExists {
				DPT[pageID] = LogRecordLocationInfo{
					Lsn:      record.lsn,
					Location: iter.Location(),
				}
			}
		case TypeUpdate:
			record := assert.Cast[UpdateLogRecord](untypedRecord)
			recordLocation := LogRecordLocationInfo{
				Lsn:      record.lsn,
				Location: iter.Location(),
			}

			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					recordLocation,
				),
			)

			pageID := record.modifiedRecordID.PageIdentity()
			_, alreadyExists := DPT[pageID]
			if !alreadyExists {
				DPT[pageID] = recordLocation
			}
		case TypeDelete:
			record := assert.Cast[DeleteLogRecord](untypedRecord)
			recordLocation := LogRecordLocationInfo{
				Lsn:      record.lsn,
				Location: iter.Location(),
			}

			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					recordLocation,
				),
			)

			pageID := record.modifiedRecordID.PageIdentity()
			_, alreadyExists := DPT[pageID]
			if !alreadyExists {
				DPT[pageID] = recordLocation
			}
		case TypeCommit:
			record := assert.Cast[CommitLogRecord](untypedRecord)
			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(
					TxnStatusCommit,
					LogRecordLocationInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
					}),
			)
		case TypeAbort:
			record := assert.Cast[AbortLogRecord](untypedRecord)

			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					LogRecordLocationInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
					}),
			)
		case TypeTxnEnd:
			record := assert.Cast[TxnEndLogRecord](untypedRecord)
			delete(ATT.table, record.txnID)
		case TypeCheckpointBegin:
			_ = assert.Cast[CheckpointBeginLogRecord](untypedRecord)
		case TypeCheckpointEnd:
			record := assert.Cast[CheckpointEndLogRecord](
				untypedRecord,
			)

			for _, TransactionID := range record.activeTransactions {
				ATT.Insert(TransactionID, TypeBegin, NewATTEntry(
					TxnStatusUndo,
					NewNilLogRecordLocation(),
				))
			}

			// Dirty Page Table (DPT):
			// The DPT contains information about the pages in the buffer pool
			// that were
			// modified by uncommitted txns. There is one entry per dirty page
			// containing the recLSN (i.e., the LSN of the log record that first
			// caused the page to be dirty).
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
			record := assert.Cast[CompensationLogRecord](untypedRecord)

			ATT.Insert(
				record.txnID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					LogRecordLocationInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
					}),
			)
		default:
			assert.Assert(
				tag < TypeUnknown,
				"unexpected log record type: %d",
				tag,
			)
			panic("unreachable")
		}

		success, err := iter.MoveForward()
		assert.NoError(err)

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

		recordLocation := entry.logLocationInfo
		lastInsertedRecordLocation := recordLocation
		clrsFound := 0
	outer:
		for {
			tag, record, err := l.readLogRecord(recordLocation.Location)
			assert.NoError(err)
			switch tag {
			case TypeBegin:
				record := assert.Cast[BeginLogRecord](record)

				if earliestLogLocation.Lsn > record.lsn {
					earliestLogLocation = recordLocation
				}
				assert.Assert(clrsFound == 0, "CLRs aren't balanced out")

				_, err := l.AppendTxnEnd(record.txnID, entry.logLocationInfo)
				assert.NoError(err)
				break outer
			case TypeInsert:
				record := assert.Cast[InsertLogRecord](record)

				DPT[record.modifiedRecordID.PageIdentity()] = recordLocation
				recordLocation = record.parentLogLocation
				if clrsFound > 0 {
					clrsFound--
					continue
				}
				_, lastInsertedRecordLocation, err = loggerUndoRecord(l, &record, lastInsertedRecordLocation)
				assert.NoError(err)
			case TypeUpdate:
				record := assert.Cast[UpdateLogRecord](record)

				DPT[record.modifiedRecordID.PageIdentity()] = recordLocation
				recordLocation = record.parentLogLocation
				if clrsFound > 0 {
					clrsFound--
					continue
				}
				_, lastInsertedRecordLocation, err = loggerUndoRecord(l, &record, lastInsertedRecordLocation)
				assert.NoError(err)
			case TypeDelete:
				record := assert.Cast[DeleteLogRecord](record)

				DPT[record.modifiedRecordID.PageIdentity()] = recordLocation
				recordLocation = record.parentLogLocation
				if clrsFound > 0 {
					clrsFound--
					continue
				}
				_, lastInsertedRecordLocation, err = loggerUndoRecord(l, &record, lastInsertedRecordLocation)
				assert.NoError(err)
			case TypeCommit:
				_ = assert.Cast[CommitLogRecord](record)

				assert.Assert(clrsFound == 0, "found CLRs for a commited txn")
				break outer
			case TypeAbort:
				record := assert.Cast[AbortLogRecord](record)
				recordLocation = record.parentLogLocation
			case TypeTxnEnd:
				_ = assert.Cast[TxnEndLogRecord](record)
				assert.Assert(tag != TypeTxnEnd, "unreachable: ATT shouldn't have log records with TxnEnd logs")
			case TypeCompensation:
				record := assert.Cast[CompensationLogRecord](record)
				clrsFound++
				recordLocation = record.parentLogLocation
			case TypeCheckpointBegin:
				_ = assert.Cast[CheckpointBeginLogRecord](record)
				assert.Assert(tag != TypeCheckpointBegin, "unreachable: ATT shouldn't have CheckpointBegin records")
			case TypeCheckpointEnd:
				_ = assert.Cast[CheckpointEndLogRecord](record)
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
	iter, err := l.iter(earliestLog)
	assert.NoError(err)

	for {
		tag, record, err := iter.ReadRecord()
		assert.NoError(err)

		switch tag {
		case TypeInsert:
			record := assert.Cast[InsertLogRecord](record)
			func() {
				modifiedPage, err := l.pool.GetPageNoCreate(
					record.modifiedRecordID.PageIdentity(),
				)
				assert.NoError(err)
				defer func() { assert.NoError(l.pool.Unpin(record.modifiedRecordID.PageIdentity())) }()

				modifiedPage.Lock()
				defer modifiedPage.Unlock()

				modifiedPage.UnsafeOverrideSlotStatus(
					record.modifiedRecordID.SlotNum,
					page.SlotStatusInserted,
				)
				slotData := modifiedPage.Read(
					record.modifiedRecordID.SlotNum,
				)

				assert.Assert(
					len(record.value) <= len(slotData),
					"new item len should be at most len of the old one",
				)

				clear(slotData)
				copy(slotData, record.value)
			}()
		case TypeUpdate:
			record := assert.Cast[UpdateLogRecord](record)
			func() {
				modifiedPage, err := l.pool.GetPageNoCreate(
					record.modifiedRecordID.PageIdentity(),
				)
				defer func() { assert.NoError(l.pool.Unpin(record.modifiedRecordID.PageIdentity())) }()

				assert.NoError(err)
				modifiedPage.Lock()
				defer modifiedPage.Unlock()

				slotData := modifiedPage.Read(
					record.modifiedRecordID.SlotNum,
				)
				assert.Assert(
					len(record.afterValue) <= len(slotData),
					"new item len should be at most len of the old one",
				)

				clear(slotData)
				copy(slotData, record.afterValue)
			}()
		case TypeDelete:
			record := assert.Cast[DeleteLogRecord](record)
			func() {
				modifiedPage, err := l.pool.GetPageNoCreate(
					record.modifiedRecordID.PageIdentity(),
				)
				assert.NoError(err)
				defer func() { assert.NoError(l.pool.Unpin(record.modifiedRecordID.PageIdentity())) }()

				modifiedPage.Lock()
				defer modifiedPage.Unlock()
				modifiedPage.UnsafeOverrideSlotStatus(
					record.modifiedRecordID.SlotNum,
					page.SlotStatusDeleted,
				)
			}()
		case TypeCompensation:
			record := assert.Cast[CompensationLogRecord](record)
			l.activateCLR(&record)
		}

		success, err := iter.MoveForward()
		assert.NoError(err)

		if !success {
			break
		}
	}
}

func (l *TxnLogger) readLogRecord(
	recordLocation FileLocation,
) (tag LogRecordTypeTag, r any, err error) {
	pageIdent := bufferpool.PageIdentity{
		FileID: l.logfileID,
		PageID: recordLocation.PageID,
	}
	page, err := l.pool.GetPage(pageIdent)
	defer func() { err = errors.Join(err, l.pool.Unpin(pageIdent)) }()

	if err != nil {
		return TypeUnknown, nil, err
	}

	page.RLock()
	record := page.Read(recordLocation.SlotNum)
	page.RUnlock()

	tag, r, err = readLogRecord(record)
	return tag, r, err
}

// writeLogRecord writes a serialized log record to the log file managed by the
// TxnLogger. It attempts to insert the record into the current log page. If
// there is not enough space on the current page, it advances to the next page
// and retries the insertion. The function returns the location information of
// the written log record or an error if the operation fails.
//
// Parameters:
//
//	serializedRecord []byte - The serialized log record to be written.
//
// Returns:
//
//	LogRecordLocationInfo - The location information of the written log record.
//	error - An error if the operation fails, otherwise nil.
func (lockedLogger *TxnLogger) writeLogRecord(
	serializedRecord []byte,
) (FileLocation, error) {
	pageInfo := bufferpool.PageIdentity{
		FileID: lockedLogger.logfileID,
		PageID: lockedLogger.lastLogLocation.Location.PageID,
	}

	p, err := lockedLogger.pool.GetPage(pageInfo)
	if err != nil {
		return FileLocation{}, err
	}
	p.Lock()
	slotNumberOpt := p.InsertPrepare(serializedRecord)
	if slotNumberOpt.IsSome() {
		slotNumber := slotNumberOpt.Unwrap()
		p.InsertCommit(slotNumber)
		p.Unlock()
		lockedLogger.lastLogLocation.Location.SlotNum = slotNumber
		err = lockedLogger.pool.Unpin(pageInfo)
		return lockedLogger.lastLogLocation.Location, err
	}
	p.Unlock()
	err = lockedLogger.pool.Unpin(pageInfo)
	if err != nil {
		return FileLocation{}, err
	}

	lockedLogger.lastLogLocation.Location.PageID++
	pageInfo.PageID++

	p, err = lockedLogger.pool.GetPage(pageInfo)
	if err != nil {
		return FileLocation{}, err
	}

	p.Lock()
	slotNumberOpt = p.InsertPrepare(serializedRecord)
	assert.Assert(
		slotNumberOpt.IsSome(),
		"impossible, because (1) the logger is locked [no concurrent writes are possible] "+
			"and (2) the newly allocated page should be empty",
	)
	p.InsertCommit(slotNumberOpt.Unwrap())
	p.Unlock()

	err = lockedLogger.pool.Unpin(pageInfo)
	lockedLogger.lastLogLocation.Location.SlotNum = slotNumberOpt.Unwrap()

	return lockedLogger.lastLogLocation.Location, err
}

func (l *TxnLogger) NewLSN() LSN {
	l.logRecordsCount++
	lsn := LSN(l.logRecordsCount)
	return lsn
}

func marshalRecordAndWrite[T LogRecord](
	lockedLogger *TxnLogger,
	record T,
) (LogRecordLocationInfo, error) {
	bytes, err := record.MarshalBinary()
	if err != nil {
		return LogRecordLocationInfo{}, err
	}

	loc, err := lockedLogger.writeLogRecord(bytes)
	if err != nil {
		return LogRecordLocationInfo{}, err
	}

	logInfo := LogRecordLocationInfo{
		Lsn:      record.LSN(),
		Location: loc,
	}

	return logInfo, nil
}

func (l *TxnLogger) AppendBegin(
	TransactionID txns.TxnID,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewBeginLogRecord(l.NewLSN(), TransactionID)
	return marshalRecordAndWrite(l, &r)
}

func (l *TxnLogger) AppendUpdate(
	TransactionID txns.TxnID,
	prevLog LogRecordLocationInfo,
	recordID RecordID,
	beforeValue []byte,
	afterValue []byte,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewUpdateLogRecord(
		l.NewLSN(),
		TransactionID,
		prevLog,
		recordID,
		beforeValue,
		afterValue,
	)
	return marshalRecordAndWrite(l, &r)
}

func loggerUndoRecord[T RevertableLogRecord](
	l *TxnLogger,
	record T,
	parentLocation LogRecordLocationInfo,
) (*CompensationLogRecord, LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	clr := record.Undo(
		l.NewLSN(),
		parentLocation,
	)
	location, err := marshalRecordAndWrite(l, &clr)
	if err != nil {
		return nil, LogRecordLocationInfo{}, err
	}

	return &clr, location, nil
}

func (l *TxnLogger) AppendInsert(
	TransactionID txns.TxnID,
	prevLog LogRecordLocationInfo,
	recordID RecordID,
	value []byte,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewInsertLogRecord(
		l.NewLSN(),
		TransactionID,
		prevLog,
		recordID,
		value,
	)
	return marshalRecordAndWrite(l, &r)
}

func (l *TxnLogger) AppendDelete(
	TransactionID txns.TxnID,
	prevLog LogRecordLocationInfo,
	recordID RecordID,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewDeleteLogRecord(
		l.NewLSN(),
		TransactionID,
		prevLog,
		recordID,
	)
	return marshalRecordAndWrite(l, &r)
}

func (l *TxnLogger) AppendCommit(
	TransactionID txns.TxnID,
	prevLog LogRecordLocationInfo,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewCommitLogRecord(l.NewLSN(), TransactionID, prevLog)
	return marshalRecordAndWrite(l, &r)
}

func (l *TxnLogger) AppendAbort(
	TransactionID txns.TxnID,
	prevLog LogRecordLocationInfo,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewAbortLogRecord(l.NewLSN(), TransactionID, prevLog)
	return marshalRecordAndWrite(l, &r)
}

func (l *TxnLogger) AppendTxnEnd(
	TransactionID txns.TxnID,
	prevLog LogRecordLocationInfo,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewTxnEndLogRecord(l.NewLSN(), TransactionID, prevLog)
	return marshalRecordAndWrite(l, &r)
}

func (l *TxnLogger) AppendCheckpointBegin() (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewCheckpointBegin(l.NewLSN())
	return marshalRecordAndWrite(l, &r)
}

func (l *TxnLogger) AppendCheckpointEnd(
	activeTransacitons []txns.TxnID,
	dirtyPageTable map[bufferpool.PageIdentity]LogRecordLocationInfo,
) (LogRecordLocationInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := NewCheckpointEnd(
		l.NewLSN(),
		activeTransacitons,
		dirtyPageTable,
	)
	return marshalRecordAndWrite(l, &r)
}

// TODO: handle insertion and deletion UNDOs
func (l *TxnLogger) activateCLR(record *CompensationLogRecord) {
	pageID := record.modifiedRecordID.PageIdentity()
	page, err := l.pool.GetPageNoCreate(pageID)
	assert.NoError(err)
	defer func() { assert.NoError(l.pool.Unpin(pageID)) }()

	page.Lock()
	defer page.Unlock()

	switch record.clrType {
	case CLRtypeInsert:
		page.Delete(record.modifiedRecordID.SlotNum)
	case CLRtypeUpdate:
		page.Update(record.modifiedRecordID.SlotNum, record.afterValue)
	case CLRtypeDelete:
		page.UndoDelete(record.modifiedRecordID.SlotNum)
	}
}

func (l *TxnLogger) Rollback(abortLogRecord LogRecordLocationInfo) {
	assert.Assert(!abortLogRecord.isNil(), "nil log record")

	_, abordRecord, err := l.readLogRecord(abortLogRecord.Location)
	assert.NoError(err)

	record := assert.Cast[AbortLogRecord](abordRecord)
	revertingRecordlocation := record.parentLogLocation
	lastInsertedRecordLocation := abortLogRecord

	clrsFound := 0
outer:
	for {
		tag, record, err := l.readLogRecord(revertingRecordlocation.Location)
		assert.NoError(err)
		switch tag {
		case TypeBegin:
			record := assert.Cast[BeginLogRecord](record)
			assert.Assert(clrsFound == 0, "CLRs aren't balanced out")
			_, err := l.AppendTxnEnd(record.txnID, abortLogRecord)
			assert.NoError(err)
			break outer
		case TypeInsert:
			record := assert.Cast[InsertLogRecord](record)

			revertingRecordlocation = record.parentLogLocation
			if clrsFound > 0 {
				clrsFound--
				continue
			}

			var clr *CompensationLogRecord
			clr, lastInsertedRecordLocation, err = loggerUndoRecord(l, &record, lastInsertedRecordLocation)
			assert.NoError(err)
			l.activateCLR(clr)
		case TypeDelete:
			record := assert.Cast[DeleteLogRecord](record)

			revertingRecordlocation = record.parentLogLocation
			if clrsFound > 0 {
				clrsFound--
				continue
			}

			var clr *CompensationLogRecord
			clr, lastInsertedRecordLocation, err = loggerUndoRecord(l, &record, lastInsertedRecordLocation)
			assert.NoError(err)
			l.activateCLR(clr)
		case TypeUpdate:
			record := assert.Cast[UpdateLogRecord](record)

			revertingRecordlocation = record.parentLogLocation
			if clrsFound > 0 {
				clrsFound--
				continue
			}

			var clr *CompensationLogRecord
			clr, lastInsertedRecordLocation, err = loggerUndoRecord(l, &record, lastInsertedRecordLocation)
			assert.NoError(err)
			l.activateCLR(clr)
		case TypeCommit:
			_ = assert.Cast[CommitLogRecord](record)
			assert.Assert(clrsFound == 0, "found CLRs for a commited txn")
			assert.Assert(tag != TypeCommit, "cannot rollback a commited txn")
		case TypeAbort:
			_ = assert.Cast[AbortLogRecord](record)
			assert.Assert(
				tag != TypeAbort,
				"found multiple abort messages",
			)
		case TypeTxnEnd:
			_ = assert.Cast[TxnEndLogRecord](record)
			assert.Assert(tag != TypeTxnEnd, "cannot rollback a commited txn")
		case TypeCompensation:
			record := assert.Cast[CompensationLogRecord](record)
			l.activateCLR(&record)
			clrsFound++
			revertingRecordlocation = record.parentLogLocation
		case TypeCheckpointBegin:
			_ = assert.Cast[CheckpointBeginLogRecord](record)
			assert.Assert(
				tag != TypeCheckpointBegin,
				"unreachable: ATT shouldn't have CheckpointBegin records",
			)
		case TypeCheckpointEnd:
			_ = assert.Cast[CheckpointEndLogRecord](record)
			assert.Assert(
				tag != TypeCheckpointEnd,
				"unexpected record type: CheckpointEndLogRecord",
			)
		default:
			assert.Assert(false, "unexpected record type: %d", tag)
			panic("unreachable")
		}
	}
}
