package recovery

import (
	"encoding"
	"fmt"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type LSN uint64

var NIL_LSN LSN = LSN(0)

type FileLocation struct {
	PageID  uint64
	SlotNum uint16
}

type RecordID struct {
	FileID  uint64
	PageID  uint64
	SlotNum uint16
}

func (r RecordID) PageIdentity() bufferpool.PageIdentity {
	return bufferpool.PageIdentity{
		FileID: r.FileID,
		PageID: r.PageID,
	}
}

func (r RecordID) FileLocation() FileLocation {
	return FileLocation{
		PageID:  r.PageID,
		SlotNum: r.SlotNum,
	}
}

// is considered NIL iff lsn is NIL_LSN
type LogRecordLocationInfo struct {
	Lsn      LSN
	Location FileLocation
}

func NewNilLogRecordLocation() LogRecordLocationInfo {
	return LogRecordLocationInfo{
		Lsn:      NIL_LSN,
		Location: FileLocation{},
	}
}

func (p *LogRecordLocationInfo) isNil() bool {
	return p.Lsn == NIL_LSN
}

type LogRecord interface {
	LSN() LSN
	String() string
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type RevertableLogRecord interface {
	LogRecord
	Undo(
		newLSN LSN,
		parentLogLocation LogRecordLocationInfo,
	) CompensationLogRecord
}

var (
	_ LogRecord           = &BeginLogRecord{}
	_ RevertableLogRecord = &InsertLogRecord{}
	_ RevertableLogRecord = &UpdateLogRecord{}
	_ RevertableLogRecord = &DeleteLogRecord{}
	_ LogRecord           = &CommitLogRecord{}
	_ LogRecord           = &AbortLogRecord{}
	_ LogRecord           = &CheckpointBeginLogRecord{}
	_ LogRecord           = &CheckpointEndLogRecord{}
	_ LogRecord           = &CompensationLogRecord{}
	_ LogRecord           = &TxnEndLogRecord{}
)

type BeginLogRecord struct {
	lsn   LSN
	txnID txns.TxnID
}

func NewBeginLogRecord(lsn LSN, txnID txns.TxnID) BeginLogRecord {
	return BeginLogRecord{
		lsn:   lsn,
		txnID: txnID,
	}
}

func (l *BeginLogRecord) LSN() LSN {
	return l.lsn
}

func (l *BeginLogRecord) String() string {
	return fmt.Sprintf("BEGIN lsn:%d txn:%d", l.lsn, l.txnID)
}

type UpdateLogRecord struct {
	lsn               LSN
	txnID             txns.TxnID
	parentLogLocation LogRecordLocationInfo
	modifiedRecordID  RecordID
	beforeValue       []byte
	afterValue        []byte
}

func (r *UpdateLogRecord) LSN() LSN {
	return r.lsn
}

func (r *UpdateLogRecord) String() string {
	return fmt.Sprintf(
		"UPDATE lsn:%d txn:%d parent:%+v modified:%+v before:%+v after:%+v",
		r.lsn,
		r.txnID,
		r.parentLogLocation,
		r.modifiedRecordID,
		r.beforeValue,
		r.afterValue,
	)
}

func (r *UpdateLogRecord) Undo(
	lsn LSN,
	parentLogLocation LogRecordLocationInfo,
) CompensationLogRecord {
	return NewCompensationLogRecord(
		lsn,
		r.txnID,
		parentLogLocation,
		r.modifiedRecordID,
		CLRtypeUpdate,
		r.parentLogLocation.Lsn,
		r.afterValue,
		r.beforeValue,
	)
}

func NewUpdateLogRecord(
	lsn LSN,
	txnID txns.TxnID,
	parentLogLocation LogRecordLocationInfo,
	modifiedRecordID RecordID,
	beforeValue []byte,
	afterValue []byte,
) UpdateLogRecord {
	assert.Assert(len(afterValue) <= len(beforeValue))

	return UpdateLogRecord{
		lsn:               lsn,
		txnID:             txnID,
		parentLogLocation: parentLogLocation,
		modifiedRecordID:  modifiedRecordID,
		beforeValue:       beforeValue,
		afterValue:        afterValue,
	}
}

type InsertLogRecord struct {
	lsn               LSN
	txnID             txns.TxnID
	parentLogLocation LogRecordLocationInfo
	modifiedRecordID  RecordID
	value             []byte
}

func (r *InsertLogRecord) LSN() LSN {
	return r.lsn
}

func (r *InsertLogRecord) String() string {
	return fmt.Sprintf(
		"INSERT lsn:%d txn:%d parent:%+v modified:%+v value:%+v",
		r.lsn,
		r.txnID,
		r.parentLogLocation,
		r.modifiedRecordID,
		r.value,
	)
}

func NewInsertLogRecord(
	lsn LSN,
	txnID txns.TxnID,
	parentLogLocation LogRecordLocationInfo,
	modifiedRecordID RecordID,
	value []byte,
) InsertLogRecord {
	return InsertLogRecord{
		lsn:               lsn,
		txnID:             txnID,
		parentLogLocation: parentLogLocation,
		modifiedRecordID:  modifiedRecordID,
		value:             value,
	}
}

func (r *InsertLogRecord) Undo(
	lsn LSN,
	parentLogLocation LogRecordLocationInfo,
) CompensationLogRecord {
	return NewCompensationLogRecord(
		lsn,
		r.txnID,
		parentLogLocation,
		r.modifiedRecordID,
		CLRtypeInsert,
		r.parentLogLocation.Lsn,
		r.value,
		make([]byte, len(r.value)),
	)
}

type DeleteLogRecord struct {
	lsn               LSN
	txnID             txns.TxnID
	parentLogLocation LogRecordLocationInfo
	modifiedRecordID  RecordID
}

func NewDeleteLogRecord(
	lsn LSN,
	txnID txns.TxnID,
	parentLogLocation LogRecordLocationInfo,
	modifiedRecordID RecordID,
) DeleteLogRecord {
	return DeleteLogRecord{
		lsn:               lsn,
		txnID:             txnID,
		parentLogLocation: parentLogLocation,
		modifiedRecordID:  modifiedRecordID,
	}
}

func (r *DeleteLogRecord) LSN() LSN {
	return r.lsn
}

func (r *DeleteLogRecord) String() string {
	return fmt.Sprintf(
		"DELETE lsn:%d txn:%d parent:%+v modified:%+v",
		r.lsn,
		r.txnID,
		r.parentLogLocation,
		r.modifiedRecordID,
	)
}

func (r *DeleteLogRecord) Undo(
	lsn LSN,
	parentLogLocation LogRecordLocationInfo,
) CompensationLogRecord {
	return NewCompensationLogRecord(
		lsn,
		r.txnID,
		parentLogLocation,
		r.modifiedRecordID,
		CLRtypeDelete,
		r.parentLogLocation.Lsn,
		make([]byte, 0),
		make([]byte, 0),
	)
}

type CommitLogRecord struct {
	lsn               LSN
	txnID             txns.TxnID
	parentLogLocation LogRecordLocationInfo
}

func (r *CommitLogRecord) LSN() LSN {
	return r.lsn
}

func NewCommitLogRecord(
	lsn LSN,
	txnID txns.TxnID,
	parentLogLocation LogRecordLocationInfo,
) CommitLogRecord {
	return CommitLogRecord{
		lsn:               lsn,
		txnID:             txnID,
		parentLogLocation: parentLogLocation,
	}
}

func (r *CommitLogRecord) String() string {
	return fmt.Sprintf(
		"COMMIT lsn:%d txn:%d parent:%+v",
		r.lsn,
		r.txnID,
		r.parentLogLocation,
	)
}

type AbortLogRecord struct {
	lsn               LSN
	txnID             txns.TxnID
	parentLogLocation LogRecordLocationInfo
}

func (r *AbortLogRecord) LSN() LSN {
	return r.lsn
}

func (r *AbortLogRecord) String() string {
	return fmt.Sprintf(
		"ABORT lsn:%d txn:%d parent:%+v",
		r.lsn,
		r.txnID,
		r.parentLogLocation,
	)
}

func NewAbortLogRecord(lsn LSN, txnID txns.TxnID,
	parentLogLocation LogRecordLocationInfo,
) AbortLogRecord {
	return AbortLogRecord{
		lsn:               lsn,
		txnID:             txnID,
		parentLogLocation: parentLogLocation,
	}
}

type TxnEndLogRecord struct {
	lsn               LSN
	txnID             txns.TxnID
	parentLogLocation LogRecordLocationInfo
}

func (r *TxnEndLogRecord) LSN() LSN {
	return r.lsn
}

func NewTxnEndLogRecord(lsn LSN, txnID txns.TxnID,
	parentLogLocation LogRecordLocationInfo) TxnEndLogRecord {
	return TxnEndLogRecord{
		lsn:               lsn,
		txnID:             txnID,
		parentLogLocation: parentLogLocation,
	}
}

func (r *TxnEndLogRecord) String() string {
	return fmt.Sprintf(
		"TXN_END lsn:%d txn:%d parent:%+v",
		r.lsn,
		r.txnID,
		r.parentLogLocation,
	)
}

type CLRtype byte

const (
	CLRtypeInsert CLRtype = iota
	CLRtypeUpdate
	CLRtypeDelete
)

type CompensationLogRecord struct {
	lsn               LSN
	txnID             txns.TxnID
	parentLogLocation LogRecordLocationInfo
	nextUndoLSN       LSN
	clrType           CLRtype
	modifiedRecordID  RecordID
	beforeValue       []byte
	afterValue        []byte
}

func (r *CompensationLogRecord) LSN() LSN {
	return r.lsn
}

func (r *CompensationLogRecord) String() string {
	return fmt.Sprintf(
		"CLR lsn:%d txn:%d parent:%+v next_undo:%+v type:%+v modified:%+v before:%+v after:%+v",
		r.lsn,
		r.txnID,
		r.parentLogLocation,
		r.nextUndoLSN,
		r.clrType,
		r.modifiedRecordID,
		r.beforeValue,
		r.afterValue,
	)
}

func NewCompensationLogRecord(
	lsn LSN,
	txnID txns.TxnID,
	parentLogLocation LogRecordLocationInfo,
	modifiedRecordID RecordID,
	clrType CLRtype,
	nextUndoLSN LSN,
	beforeValue []byte,
	afterValue []byte,
) CompensationLogRecord {
	return CompensationLogRecord{
		lsn:               lsn,
		txnID:             txnID,
		parentLogLocation: parentLogLocation,
		modifiedRecordID:  modifiedRecordID,
		clrType:           clrType,
		nextUndoLSN:       nextUndoLSN,
		beforeValue:       beforeValue,
		afterValue:        afterValue,
	}
}

func (r *CompensationLogRecord) Activate() {

}

type CheckpointBeginLogRecord struct {
	lsn LSN
}

func NewCheckpointBegin(lsn LSN) CheckpointBeginLogRecord {
	return CheckpointBeginLogRecord{lsn: lsn}
}

func (l *CheckpointBeginLogRecord) LSN() LSN {
	return l.lsn
}

func (l *CheckpointBeginLogRecord) String() string {
	return fmt.Sprintf("CHECKPOINT_BEGIN lsn:%d", l.lsn)
}

type CheckpointEndLogRecord struct {
	lsn                LSN
	activeTransactions []txns.TxnID
	dirtyPageTable     map[bufferpool.PageIdentity]LogRecordLocationInfo
}

func NewCheckpointEnd(
	lsn LSN,
	activeTransacitons []txns.TxnID,
	dirtyPageTable map[bufferpool.PageIdentity]LogRecordLocationInfo,
) CheckpointEndLogRecord {
	return CheckpointEndLogRecord{
		lsn:                lsn,
		activeTransactions: activeTransacitons,
		dirtyPageTable:     dirtyPageTable,
	}
}

func (l *CheckpointEndLogRecord) LSN() LSN {
	return l.lsn
}

func (l *CheckpointEndLogRecord) String() string {
	return fmt.Sprintf("CHECKPOINT_END lsn:%d active_txns:%v dirty_pages:%v",
		l.lsn, l.activeTransactions, l.dirtyPageTable)
}
