package recovery

import (
	"encoding"
	"errors"
	"sync/atomic"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/transactions"
)

type TxnLogger struct {
	pool bufferpool.BufferPool[*page.SlottedPage]

	logfileID     uint64
	currentPageID atomic.Uint64

	logRecordsCount atomic.Uint64 // "где-то" лежит на диске

	getActiveTransactions func() []transactions.TxnID  // Придет из лок менеджера
}

/*
 * TODO: Разобраться где именн хранить
 * 1. точку начала (№ страницы лог файла) последнего чекпоинта
 *    Не обязательно сразу флашить на диск. Обязательно флашим
 *    точку оканчания чекпоинта <---- откуда восстанавливаться
 * 2. № страницы последней записи <---- куда начать писать после инициализации (флашить НЕ обязательно)
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
		p, err := l.pool.GetPage(l.logfileID, curPageID)
		if err != nil {
			return err
		}

		p.Lock()
		_, err = p.Insert(bytes)
		p.Unlock()
		if !errors.Is(err, page.ErrNoEnoughSpace) {
			break
		}

		l.pool.Unpin(l.logfileID, curPageID)
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
