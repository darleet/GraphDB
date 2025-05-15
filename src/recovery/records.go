package recovery

import "github.com/Blackdeer1524/GraphDB/src/bufferpool"

type TransactionID uint64
type LSN uint64

var NIL_LSN LSN = LSN(0)

type BeginLogRecord struct {
	lsn   LSN
	txnId TransactionID
}

func NewBeginLogRecord(lsn LSN, txnId TransactionID) BeginLogRecord {
	return BeginLogRecord{
		lsn:   lsn,
		txnId: txnId,
	}
}

type UpdateLogRecord struct {
	lsn         LSN
	txnId       TransactionID
	prevLSN     LSN
	pageInfo    bufferpool.PageIdentity
	slotNumber  int
	beforeValue []byte
	afterValue  []byte
}

func NewUpdateLogRecord(
	lsn LSN,
	txnId TransactionID,
	prevLSN LSN,
	pageInfo bufferpool.PageIdentity,
	slotNumber int,
	beforeValue []byte,
	afterValue []byte,
) UpdateLogRecord {
	return UpdateLogRecord{
		lsn:         lsn,
		txnId:       txnId,
		prevLSN:     prevLSN,
		pageInfo:    pageInfo,
		slotNumber:  slotNumber,
		beforeValue: beforeValue,
		afterValue:  afterValue,
	}
}

type InsertLogRecord struct {
	lsn        LSN
	txnId      TransactionID
	prevLSN    LSN
	pageInfo   bufferpool.PageIdentity
	slotNumber int
	value      []byte
}

func NewInsertLogRecord(
	lsn LSN,
	txnId TransactionID,
	prevLSN LSN,
	pageInfo bufferpool.PageIdentity,
	slotNumber int,
	value []byte,
) InsertLogRecord {
	return InsertLogRecord{
		lsn:        lsn,
		txnId:      txnId,
		prevLSN:    prevLSN,
		pageInfo:   pageInfo,
		slotNumber: slotNumber,
		value:      value,
	}
}

type CommitLogRecord struct {
	lsn     LSN
	txnId   TransactionID
	prevLSN LSN
}

func NewCommitLogRecord(lsn LSN, txnId TransactionID, prevLSN LSN) CommitLogRecord {
	return CommitLogRecord{
		lsn:     lsn,
		txnId:   txnId,
		prevLSN: prevLSN,
	}
}

type AbortLogRecord struct {
	lsn     LSN
	txnId   TransactionID
	prevLSN LSN
}

func NewAbortLogRecord(lsn LSN, txnId TransactionID, prevLSN LSN) AbortLogRecord {
	return AbortLogRecord{
		lsn:     lsn,
		txnId:   txnId,
		prevLSN: prevLSN,
	}
}

type TxnEndLogRecord struct {
	lsn     LSN
	txnId   TransactionID
	prevLSN LSN
}

func NewTxnEndLogRecord(lsn LSN, txnId TransactionID, prevLSN LSN) TxnEndLogRecord {
	return TxnEndLogRecord{
		lsn:     lsn,
		txnId:   txnId,
		prevLSN: prevLSN,
	}
}

type CompensationLogRecord struct {
	lsn         LSN
	txnId       TransactionID
	prevLSN     LSN
	pageInfo    bufferpool.PageIdentity
	nextUndoLSN LSN
	slotNumber  int
	beforeValue []byte
	afterValue  []byte
}

func NewCompensationLogRecord(
	lsn LSN,
	txnId TransactionID,
	prevLSN LSN,
	pageInfo bufferpool.PageIdentity,
	nextUndoLSN LSN,
	slotNumber int,
	beforeValue []byte,
	afterValue []byte,
) CompensationLogRecord {
	return CompensationLogRecord{
		lsn:         lsn,
		txnId:       txnId,
		prevLSN:     prevLSN,
		pageInfo:    pageInfo,
		nextUndoLSN: nextUndoLSN,
		slotNumber:  slotNumber,
		beforeValue: beforeValue,
		afterValue:  afterValue,
	}
}
