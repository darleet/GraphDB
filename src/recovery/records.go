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

type PageLocation struct {
	PageID  uint64
	SlotNum uint32
}

// is considered NIL iff lsn is NIL_LSN
type LogRecordLocation struct {
	Lsn     LSN
	PageLoc PageLocation
}

func NewNilLogRecordLocation() LogRecordLocation {
	return LogRecordLocation{
		Lsn:     NIL_LSN,
		PageLoc: PageLocation{},
	}
}

func (p *LogRecordLocation) isNil() bool {
	return p.Lsn == NIL_LSN
}

type UpdateLogRecord struct {
	lsn                LSN
	txnId              transactions.TxnID
	prevLogLocation    LogRecordLocation
	modifiedPageInfo   bufferpool.PageIdentity
	modifiedSlotNumber uint32
	beforeValue        []byte
	afterValue         []byte
}

func (r *UpdateLogRecord) Undo(lsn LSN, prevLogLocation LogRecordLocation) CompensationLogRecord {
	return NewCompensationLogRecord(
		lsn,
		r.txnId,
		prevLogLocation,
		r.modifiedPageInfo,
		r.modifiedSlotNumber,
		false,
		r.prevLogLocation.Lsn,
		r.afterValue,
		r.beforeValue,
	)
}

func NewUpdateLogRecord(
	lsn LSN,
	txnId transactions.TxnID,
	prevLogLocation LogRecordLocation,
	modifiedPageInfo bufferpool.PageIdentity,
	modifiedSlotNumber uint32,
	beforeValue []byte,
	afterValue []byte,
) UpdateLogRecord {
	return UpdateLogRecord{
		lsn:                lsn,
		txnId:              txnId,
		prevLogLocation:    prevLogLocation,
		modifiedPageInfo:   modifiedPageInfo,
		modifiedSlotNumber: modifiedSlotNumber,
		beforeValue:        beforeValue,
		afterValue:         afterValue,
	}
}

type InsertLogRecord struct {
	lsn                LSN
	txnId              transactions.TxnID
	prevLogLocation    LogRecordLocation
	modifiedPageInfo   bufferpool.PageIdentity
	modifiedSlotNumber uint32
	value              []byte
}

func NewInsertLogRecord(
	lsn LSN,
	txnId transactions.TxnID,
	prevLogLocation LogRecordLocation,
	modifiedPageInfo bufferpool.PageIdentity,
	modifiedSlotNumber uint32,
	value []byte,
) InsertLogRecord {
	return InsertLogRecord{
		lsn:                lsn,
		txnId:              txnId,
		prevLogLocation:    prevLogLocation,
		modifiedPageInfo:   modifiedPageInfo,
		modifiedSlotNumber: modifiedSlotNumber,
		value:              value,
	}
}

func (r *InsertLogRecord) Undo(lsn LSN, prevLogLocation LogRecordLocation) CompensationLogRecord {
	return NewCompensationLogRecord(
		lsn,
		r.txnId,
		prevLogLocation,
		r.modifiedPageInfo,
		r.modifiedSlotNumber,
		true,
		r.prevLogLocation.Lsn,
		r.value,
		make([]byte, len(r.value)),
	)
}

type CommitLogRecord struct {
	lsn             LSN
	txnId           transactions.TxnID
	prevLogLocation LogRecordLocation
}

func NewCommitLogRecord(lsn LSN, txnId transactions.TxnID, prevLogLocation LogRecordLocation) CommitLogRecord {
	return CommitLogRecord{
		lsn:             lsn,
		txnId:           txnId,
		prevLogLocation: prevLogLocation,
	}
}

type AbortLogRecord struct {
	lsn             LSN
	txnId           transactions.TxnID
	prevLogLocation LogRecordLocation
}

func NewAbortLogRecord(lsn LSN, txnId transactions.TxnID,
	prevLogLocation LogRecordLocation,
) AbortLogRecord {
	return AbortLogRecord{
		lsn:             lsn,
		txnId:           txnId,
		prevLogLocation: prevLogLocation,
	}
}

type TxnEndLogRecord struct {
	lsn             LSN
	txnId           transactions.TxnID
	prevLogLocation LogRecordLocation
}

func NewTxnEndLogRecord(lsn LSN, txnId transactions.TxnID,
	prevLogLocation LogRecordLocation) TxnEndLogRecord {
	return TxnEndLogRecord{
		lsn:             lsn,
		txnId:           txnId,
		prevLogLocation: prevLogLocation,
	}
}

type CompensationLogRecord struct {
	lsn                LSN
	txnId              transactions.TxnID
	prevLogLocation    LogRecordLocation
	nextUndoLSN        LSN
	isDelete           bool
	modifiedPageInfo   bufferpool.PageIdentity
	modifiedSlotNumber uint32
	beforeValue        []byte
	afterValue         []byte
}

func NewCompensationLogRecord(
	lsn LSN,
	txnId transactions.TxnID,
	prevLogLocation LogRecordLocation,
	modifiedPageInfo bufferpool.PageIdentity,
	modifiedSlotNumber uint32,
	isDelete bool,
	nextUndoLSN LSN,
	beforeValue []byte,
	afterValue []byte,
) CompensationLogRecord {
	return CompensationLogRecord{
		lsn:                lsn,
		txnId:              txnId,
		prevLogLocation:    prevLogLocation,
		modifiedPageInfo:   modifiedPageInfo,
		modifiedSlotNumber: modifiedSlotNumber,
		isDelete:           isDelete,
		nextUndoLSN:        nextUndoLSN,
		beforeValue:        beforeValue,
		afterValue:         afterValue,
	}
}

type CheckpointBeginLogRecord struct {
	lsn LSN
}

func NewCheckpointBegin(lsn LSN) CheckpointBeginLogRecord {
	return CheckpointBeginLogRecord{lsn: lsn}
}

type CheckpointEndLogRecord struct {
	lsn                LSN
	activeTransactions []transactions.TxnID
	dirtyPageTable     map[bufferpool.PageIdentity]LogRecordLocation
}

func NewCheckpointEnd(
	lsn LSN,
	activeTransacitons []transactions.TxnID,
	dirtyPageTable map[bufferpool.PageIdentity]LogRecordLocation,
) CheckpointEndLogRecord {
	return CheckpointEndLogRecord{
		lsn:                lsn,
		activeTransactions: activeTransacitons,
		dirtyPageTable:     dirtyPageTable,
	}

}
