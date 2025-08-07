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

func (lockedLogger *TxnLogger) iter(
	start FileLocation,
) (*LogRecordsIter, error) {
	p, err := lockedLogger.pool.GetPageNoCreate(bufferpool.PageIdentity{
		FileID: lockedLogger.logfileID,
		PageID: start.PageID,
	})
	if err != nil {
		return nil, err
	}

	p.RLock()
	iter := newLogRecordIter(
		lockedLogger.logfileID,
		start,
		lockedLogger.pool,
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

func (lockedLogger *TxnLogger) Recover(checkpointLocation FileLocation) {
	ATT, DPT := lockedLogger.recoverAnalyze(checkpointLocation)
	earliestLog := lockedLogger.recoverPrepareCLRs(ATT, DPT)
	lockedLogger.recoverRedo(earliestLog.Location)
}

func (lockedLogger *TxnLogger) recoverAnalyze(
	checkpointLocation FileLocation,
) (ActiveTransactionsTable, map[bufferpool.PageIdentity]LogRecordLocationInfo) {
	iter, err := lockedLogger.iter(FileLocation{
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
				record.TransactionID,
				tag,
				NewATTEntry(
					TxnStatusUndo,
					LogRecordLocationInfo{
						Lsn:      record.lsn,
						Location: iter.Location(),
					}),
			), "Found a `begin` record for the already running transaction. TransactionID: %d", record.TransactionID)
		case TypeInsert:
			record := assert.Cast[InsertLogRecord](untypedRecord)

			ATT.Insert(
				record.TransactionID,
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
				record.TransactionID,
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
				record.TransactionID,
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
				record.TransactionID,
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
			delete(ATT.table, record.TransactionID)
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
				record.TransactionID,
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
		assert.Assert(err == nil, "%+v", err)

		if !success {
			break
		}
	}

	return ATT, DPT
}

func (lockedLogger *TxnLogger) recoverPrepareCLRs(
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
			tag, record, err := lockedLogger.readLogRecord(recordLocation)
			assert.Assert(err == nil, "todo")
			switch tag {
			case TypeBegin:
				record := assert.Cast[BeginLogRecord](record)

				if earliestLogLocation.Lsn > record.lsn {
					earliestLogLocation.Lsn = record.lsn
					earliestLogLocation.Location = recordLocation
				}
				assert.Assert(clrsFound == 0, "CLRs aren't balanced out")

				_, err := lockedLogger.AppendTxnEnd(record.TransactionID, entry.logLocationInfo)
				assert.Assert(err == nil, "todo")
				break outer
			case TypeInsert:
				record := assert.Cast[InsertLogRecord](record)

				DPT[record.modifiedRecordID.PageIdentity()] = LogRecordLocationInfo{
					Lsn:      record.lsn,
					Location: recordLocation,
				}
				recordLocation = record.parentLogLocation.Location
				if clrsFound > 0 {
					clrsFound--
					continue
				}
				_, err := lockedLogger.undoInsert(&record)
				assert.Assert(err == nil, "todo")
			case TypeUpdate:
				record := assert.Cast[UpdateLogRecord](record)

				DPT[record.modifiedRecordID.PageIdentity()] = LogRecordLocationInfo{
					Lsn:      record.lsn,
					Location: recordLocation,
				}
				recordLocation = record.parentLogLocation.Location
				if clrsFound > 0 {
					clrsFound--
					continue
				}
				_, err := lockedLogger.undoUpdate(&record)
				assert.Assert(err == nil, "todo")
			case TypeCommit:
				_ = assert.Cast[CommitLogRecord](record)

				assert.Assert(clrsFound == 0, "found CLRs for a commited txn")
				break outer
			case TypeAbort:
				record := assert.Cast[AbortLogRecord](record)
				recordLocation = record.parentLogLocation.Location
			case TypeTxnEnd:
				_ = assert.Cast[TxnEndLogRecord](record)
				assert.Assert(tag != TypeTxnEnd, "unreachable: ATT shouldn't have log records with TxnEnd logs")
			case TypeCompensation:
				record := assert.Cast[CompensationLogRecord](record)
				clrsFound++
				recordLocation = record.parentLogLocation.Location
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

func getSlotFromPage(
	pool bufferpool.BufferPool[*page.SlottedPage],
	recordID RecordID,
) (data []byte, err error) {
	pageID := bufferpool.PageIdentity{
		FileID: recordID.FileID,
		PageID: recordID.PageID,
	}
	p, err := pool.GetPage(pageID)
	defer func() { err = pool.Unpin(pageID) }()
	assert.Assert(err == nil, "todo")

	p.Lock()
	defer p.Unlock()

	data = p.GetBytes(recordID.SlotNum)
	return
}

func (lockedLogger *TxnLogger) recoverRedo(earliestLog FileLocation) {
	iter, err := lockedLogger.iter(earliestLog)
	assert.Assert(err == nil, "todo")

	for {
		tag, record, err := iter.ReadRecord()
		assert.Assert(err == nil, "todo")

		switch tag {
		case TypeInsert:
			record := assert.Cast[InsertLogRecord](record)

			slotData, err := getSlotFromPage(
				lockedLogger.pool,
				record.modifiedRecordID,
			)
			assert.Assert(err == nil, "todo")
			assert.Assert(
				len(record.value) <= len(slotData),
				"new item len should be at most len of the old one",
			)

			clear(slotData)
			copy(slotData, record.value)
		case TypeUpdate:
			record := assert.Cast[UpdateLogRecord](record)

			slotData, err := getSlotFromPage(
				lockedLogger.pool,
				record.modifiedRecordID,
			)
			assert.Assert(err == nil, "todo")
			assert.Assert(
				len(record.afterValue) <= len(slotData),
				"length should be the same",
			)

			clear(slotData)
			copy(slotData, record.afterValue)
		case TypeCompensation:
			record := assert.Cast[CompensationLogRecord](record)
			lockedLogger.activateCLR(&record)
		}

		success, err := iter.MoveForward()
		assert.Assert(err == nil, "todo")

		if !success {
			break
		}
	}
}

func (lockedLogger *TxnLogger) readLogRecord(
	recordLocation FileLocation,
) (tag LogRecordTypeTag, r any, err error) {
	pageIdent := bufferpool.PageIdentity{
		FileID: lockedLogger.logfileID,
		PageID: recordLocation.PageID,
	}
	page, err := lockedLogger.pool.GetPage(pageIdent)
	defer func() { err = errors.Join(err, lockedLogger.pool.Unpin(pageIdent)) }()

	if err != nil {
		return TypeUnknown, nil, err
	}

	page.RLock()
	record := page.GetBytes(recordLocation.SlotNum)
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

func (lockedLogger *TxnLogger) NewLSN() LSN {
	lockedLogger.logRecordsCount++
	lsn := LSN(lockedLogger.logRecordsCount)
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

func (lockedLogger *TxnLogger) AppendBegin(
	TransactionID txns.TxnID,
) (LogRecordLocationInfo, error) {
	lockedLogger.mu.Lock()
	defer lockedLogger.mu.Unlock()

	r := NewBeginLogRecord(lockedLogger.NewLSN(), TransactionID)
	return marshalRecordAndWrite(lockedLogger, &r)
}

func (lockedLogger *TxnLogger) AppendUpdate(
	TransactionID txns.TxnID,
	prevLog LogRecordLocationInfo,
	recordID RecordID,
	beforeValue []byte,
	afterValue []byte,
) (LogRecordLocationInfo, error) {
	lockedLogger.mu.Lock()
	defer lockedLogger.mu.Unlock()

	r := NewUpdateLogRecord(
		lockedLogger.NewLSN(),
		TransactionID,
		prevLog,
		recordID,
		beforeValue,
		afterValue,
	)
	return marshalRecordAndWrite(lockedLogger, &r)
}

func (lockedLogger *TxnLogger) undoUpdate(
	updateRecord *UpdateLogRecord,
) (*CompensationLogRecord, error) {
	lockedLogger.mu.Lock()
	defer lockedLogger.mu.Unlock()

	clr := NewCompensationLogRecord(
		lockedLogger.NewLSN(),
		updateRecord.TransactionID,
		lockedLogger.lastLogLocation,
		updateRecord.modifiedRecordID,
		false,
		updateRecord.parentLogLocation.Lsn,
		updateRecord.afterValue,
		updateRecord.beforeValue,
	)

	bytes, err := clr.MarshalBinary()
	if err != nil {
		return nil, err
	}

	_, err = lockedLogger.writeLogRecord(bytes)
	if err != nil {
		return nil, err
	}

	return &clr, nil
}

func (lockedLogger *TxnLogger) AppendInsert(
	TransactionID txns.TxnID,
	prevLog LogRecordLocationInfo,
	recordID RecordID,
	value []byte,
) (LogRecordLocationInfo, error) {
	lockedLogger.mu.Lock()
	defer lockedLogger.mu.Unlock()

	r := NewInsertLogRecord(
		lockedLogger.NewLSN(),
		TransactionID,
		prevLog,
		recordID,
		value,
	)

	return marshalRecordAndWrite(lockedLogger, &r)
}

func (lockedLogger *TxnLogger) undoInsert(
	insertRecord *InsertLogRecord,
) (*CompensationLogRecord, error) {
	lockedLogger.mu.Lock()
	defer lockedLogger.mu.Unlock()

	clr := NewCompensationLogRecord(
		lockedLogger.NewLSN(),
		insertRecord.TransactionID,
		lockedLogger.lastLogLocation,
		insertRecord.modifiedRecordID,
		true,
		insertRecord.parentLogLocation.Lsn,
		insertRecord.value,
		[]byte{},
	)

	bytes, err := clr.MarshalBinary()
	if err != nil {
		return nil, err
	}

	_, err = lockedLogger.writeLogRecord(bytes)
	if err != nil {
		return nil, err
	}

	return &clr, nil
}

func (lockedLogger *TxnLogger) AppendCommit(
	TransactionID txns.TxnID,
	prevLog LogRecordLocationInfo,
) (LogRecordLocationInfo, error) {
	lockedLogger.mu.Lock()
	defer lockedLogger.mu.Unlock()

	r := NewCommitLogRecord(lockedLogger.NewLSN(), TransactionID, prevLog)
	return marshalRecordAndWrite(lockedLogger, &r)
}

func (lockedLogger *TxnLogger) AppendAbort(
	TransactionID txns.TxnID,
	prevLog LogRecordLocationInfo,
) (LogRecordLocationInfo, error) {
	lockedLogger.mu.Lock()
	defer lockedLogger.mu.Unlock()

	r := NewAbortLogRecord(lockedLogger.NewLSN(), TransactionID, prevLog)
	return marshalRecordAndWrite(lockedLogger, &r)
}

func (lockedLogger *TxnLogger) AppendTxnEnd(
	TransactionID txns.TxnID,
	prevLog LogRecordLocationInfo,
) (LogRecordLocationInfo, error) {
	lockedLogger.mu.Lock()
	defer lockedLogger.mu.Unlock()

	r := NewTxnEndLogRecord(lockedLogger.NewLSN(), TransactionID, prevLog)
	return marshalRecordAndWrite(lockedLogger, &r)
}

func (lockedLogger *TxnLogger) AppendCheckpointBegin() (LogRecordLocationInfo, error) {
	lockedLogger.mu.Lock()
	defer lockedLogger.mu.Unlock()

	r := NewCheckpointBegin(lockedLogger.NewLSN())
	return marshalRecordAndWrite(lockedLogger, &r)
}

func (lockedLogger *TxnLogger) AppendCheckpointEnd(
	activeTransacitons []txns.TxnID,
	dirtyPageTable map[bufferpool.PageIdentity]LogRecordLocationInfo,
) (LogRecordLocationInfo, error) {
	lockedLogger.mu.Lock()
	defer lockedLogger.mu.Unlock()

	r := NewCheckpointEnd(
		lockedLogger.NewLSN(),
		activeTransacitons,
		dirtyPageTable,
	)
	return marshalRecordAndWrite(lockedLogger, &r)
}

// TODO: handle insertion and deletion UNDOs
func (lockedLogger *TxnLogger) activateCLR(record *CompensationLogRecord) {
	slotData, err := getSlotFromPage(
		lockedLogger.pool,
		record.modifiedRecordID,
	)

	assert.Assert(err == nil, "todo")
	assert.Assert(
		len(record.afterValue) <= len(slotData),
		"lengths should be the same",
	)

	clear(slotData)
	copy(slotData, record.afterValue)
}

func (lockedLogger *TxnLogger) Rollback(abortLogRecord LogRecordLocationInfo) {
	assert.Assert(!abortLogRecord.isNil(), "nil log record")

	_, abordRecord, err := lockedLogger.readLogRecord(abortLogRecord.Location)
	assert.Assert(err == nil, "todo")

	record := assert.Cast[AbortLogRecord](abordRecord)
	recordLocation := record.parentLogLocation.Location

	clrsFound := 0
outer:
	for {
		tag, record, err := lockedLogger.readLogRecord(recordLocation)
		assert.Assert(err == nil, "todo")
		switch tag {
		case TypeBegin:
			record := assert.Cast[BeginLogRecord](record)
			assert.Assert(clrsFound == 0, "CLRs aren't balanced out")
			_, err := lockedLogger.AppendTxnEnd(record.TransactionID, abortLogRecord)
			assert.Assert(err == nil, "todo")
			break outer
		case TypeInsert:
			record := assert.Cast[InsertLogRecord](record)

			recordLocation = record.parentLogLocation.Location
			if clrsFound > 0 {
				clrsFound--
				continue
			}

			clr, err := lockedLogger.undoInsert(&record)
			assert.Assert(err == nil, "todo")
			lockedLogger.activateCLR(clr)
		case TypeUpdate:
			record := assert.Cast[UpdateLogRecord](record)

			recordLocation = record.parentLogLocation.Location
			if clrsFound > 0 {
				clrsFound--
				continue
			}

			clr, err := lockedLogger.undoUpdate(&record)
			assert.Assert(err == nil, "todo")
			lockedLogger.activateCLR(clr)
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
			lockedLogger.activateCLR(&record)
			clrsFound++
			recordLocation = record.parentLogLocation.Location
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
