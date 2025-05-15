package recovery

import (
	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/transactions"
)

type LSN uint64

var NIL_LSN LSN = LSN(0)

type BeginLogRecord struct {
	lsn   LSN
	txnId transactions.TxnID
}

func NewBeginLogRecord(lsn LSN, txnId transactions.TxnID) BeginLogRecord {
	return BeginLogRecord{
		lsn:   lsn,
		txnId: txnId,
	}
}

// is considered NIL iff lsn is NIL_LSN
type LogRecordLocation struct {
	Lsn    LSN
	PageID uint64
	SlotID uint32
}

func (p *LogRecordLocation) isNil() bool {
	return p.Lsn == NIL_LSN
}

type UpdateLogRecord struct {
	lsn         LSN
	txnId       transactions.TxnID
	prevLog     LogRecordLocation
	pageInfo    bufferpool.PageIdentity
	slotNumber  uint32
	beforeValue []byte
	afterValue  []byte
}

func NewUpdateLogRecord(
	lsn LSN,
	txnId transactions.TxnID,
	prevLog LogRecordLocation,
	pageInfo bufferpool.PageIdentity,
	slotNumber uint32,
	beforeValue []byte,
	afterValue []byte,
) UpdateLogRecord {
	return UpdateLogRecord{
		lsn:         lsn,
		txnId:       txnId,
		prevLog:     prevLog,
		pageInfo:    pageInfo,
		slotNumber:  slotNumber,
		beforeValue: beforeValue,
		afterValue:  afterValue,
	}
}

type InsertLogRecord struct {
	lsn        LSN
	txnId      transactions.TxnID
	prevLog    LogRecordLocation
	pageInfo   bufferpool.PageIdentity
	slotNumber uint32
	value      []byte
}

func NewInsertLogRecord(
	lsn LSN,
	txnId transactions.TxnID,
	prevLog LogRecordLocation,
	pageInfo bufferpool.PageIdentity,
	slotNumber uint32,
	value []byte,
) InsertLogRecord {
	return InsertLogRecord{
		lsn:        lsn,
		txnId:      txnId,
		prevLog:    prevLog,
		pageInfo:   pageInfo,
		slotNumber: slotNumber,
		value:      value,
	}
}

type CommitLogRecord struct {
	lsn     LSN
	txnId   transactions.TxnID
	prevLog LogRecordLocation
}

func NewCommitLogRecord(lsn LSN, txnId transactions.TxnID, prevLog LogRecordLocation) CommitLogRecord {
	return CommitLogRecord{
		lsn:     lsn,
		txnId:   txnId,
		prevLog: prevLog,
	}
}

type AbortLogRecord struct {
	lsn     LSN
	txnId   transactions.TxnID
	prevLog LogRecordLocation
}

func NewAbortLogRecord(lsn LSN, txnId transactions.TxnID,
	prevLog LogRecordLocation,
) AbortLogRecord {
	return AbortLogRecord{
		lsn:     lsn,
		txnId:   txnId,
		prevLog: prevLog,
	}
}

type TxnEndLogRecord struct {
	lsn     LSN
	txnId   transactions.TxnID
	prevLog LogRecordLocation
}

func NewTxnEndLogRecord(lsn LSN, txnId transactions.TxnID,
	prevLog LogRecordLocation) TxnEndLogRecord {
	return TxnEndLogRecord{
		lsn:     lsn,
		txnId:   txnId,
		prevLog: prevLog,
	}
}

type CompensationLogRecord struct {
	lsn         LSN
	txnId       transactions.TxnID
	prevLog     LogRecordLocation
	nextUndoLSN LSN
	pageInfo    bufferpool.PageIdentity
	slotNumber  uint32
	beforeValue []byte
	afterValue  []byte
}

func NewCompensationLogRecord(
	lsn LSN,
	txnId transactions.TxnID,
	prevLog LogRecordLocation,
	pageInfo bufferpool.PageIdentity,
	slotNumber uint32,
	nextUndoLSN LSN,
	beforeValue []byte,
	afterValue []byte,
) CompensationLogRecord {
	return CompensationLogRecord{
		lsn:         lsn,
		txnId:       txnId,
		prevLog:     prevLog,
		pageInfo:    pageInfo,
		nextUndoLSN: nextUndoLSN,
		slotNumber:  slotNumber,
		beforeValue: beforeValue,
		afterValue:  afterValue,
	}
}

type CheckpointBegin struct {
	lsn LSN
}

func NewCheckpointBegin(lsn LSN) CheckpointBegin {
	return CheckpointBegin{lsn: lsn}
}

type CheckpointEnd struct {
	lsn                LSN
	activeTransactions []transactions.TxnID
	dirtyPageTable     map[bufferpool.PageIdentity]LSN
}

func NewCheckpointEnd(
	lsn LSN,
	activeTransacitons []transactions.TxnID,
	dirtyPageTable map[bufferpool.PageIdentity]LSN,
) CheckpointEnd {
	return CheckpointEnd{
		lsn:                lsn,
		activeTransactions: activeTransacitons,
		dirtyPageTable:     dirtyPageTable,
	}

}
