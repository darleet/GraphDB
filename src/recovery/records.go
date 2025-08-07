package recovery

import (
	"encoding"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
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
	_ LogRecord           = &CommitLogRecord{}
	_ LogRecord           = &AbortLogRecord{}
	_ LogRecord           = &CheckpointBeginLogRecord{}
	_ LogRecord           = &CheckpointEndLogRecord{}
	_ LogRecord           = &CompensationLogRecord{}
)

type BeginLogRecord struct {
	lsn           LSN
	TransactionID txns.TxnID
}

func NewBeginLogRecord(lsn LSN, TransactionID txns.TxnID) BeginLogRecord {
	return BeginLogRecord{
		lsn:           lsn,
		TransactionID: TransactionID,
	}
}

func (l *BeginLogRecord) LSN() LSN {
	return l.lsn
}

type UpdateLogRecord struct {
	lsn               LSN
	TransactionID     txns.TxnID
	parentLogLocation LogRecordLocationInfo
	modifiedRecordID  RecordID
	beforeValue       []byte
	afterValue        []byte
}

func (r *UpdateLogRecord) LSN() LSN {
	return r.lsn
}

func (r *UpdateLogRecord) Undo(
	lsn LSN,
	parentLogLocation LogRecordLocationInfo,
) CompensationLogRecord {
	return NewCompensationLogRecord(
		lsn,
		r.TransactionID,
		parentLogLocation,
		r.modifiedRecordID,
		false,
		r.parentLogLocation.Lsn,
		r.afterValue,
		r.beforeValue,
	)
}

func NewUpdateLogRecord(
	lsn LSN,
	TransactionID txns.TxnID,
	parentLogLocation LogRecordLocationInfo,
	modifiedRecordID RecordID,
	beforeValue []byte,
	afterValue []byte,
) UpdateLogRecord {
	return UpdateLogRecord{
		lsn:               lsn,
		TransactionID:     TransactionID,
		parentLogLocation: parentLogLocation,
		modifiedRecordID:  modifiedRecordID,
		beforeValue:       beforeValue,
		afterValue:        afterValue,
	}
}

type InsertLogRecord struct {
	lsn               LSN
	TransactionID     txns.TxnID
	parentLogLocation LogRecordLocationInfo
	modifiedRecordID  RecordID
	value             []byte
}

func (r *InsertLogRecord) LSN() LSN {
	return r.lsn
}

func NewInsertLogRecord(
	lsn LSN,
	TransactionID txns.TxnID,
	parentLogLocation LogRecordLocationInfo,
	modifiedRecordID RecordID,
	value []byte,
) InsertLogRecord {
	return InsertLogRecord{
		lsn:               lsn,
		TransactionID:     TransactionID,
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
		r.TransactionID,
		parentLogLocation,
		r.modifiedRecordID,
		true,
		r.parentLogLocation.Lsn,
		r.value,
		make([]byte, len(r.value)),
	)
}

type CommitLogRecord struct {
	lsn               LSN
	TransactionID     txns.TxnID
	parentLogLocation LogRecordLocationInfo
}

func (r *CommitLogRecord) LSN() LSN {
	return r.lsn
}

func NewCommitLogRecord(
	lsn LSN,
	TransactionID txns.TxnID,
	parentLogLocation LogRecordLocationInfo,
) CommitLogRecord {
	return CommitLogRecord{
		lsn:               lsn,
		TransactionID:     TransactionID,
		parentLogLocation: parentLogLocation,
	}
}

type AbortLogRecord struct {
	lsn               LSN
	TransactionID     txns.TxnID
	parentLogLocation LogRecordLocationInfo
}

func (r *AbortLogRecord) LSN() LSN {
	return r.lsn
}

func NewAbortLogRecord(lsn LSN, TransactionID txns.TxnID,
	parentLogLocation LogRecordLocationInfo,
) AbortLogRecord {
	return AbortLogRecord{
		lsn:               lsn,
		TransactionID:     TransactionID,
		parentLogLocation: parentLogLocation,
	}
}

type TxnEndLogRecord struct {
	lsn               LSN
	TransactionID     txns.TxnID
	parentLogLocation LogRecordLocationInfo
}

func (r *TxnEndLogRecord) LSN() LSN {
	return r.lsn
}

func NewTxnEndLogRecord(lsn LSN, TransactionID txns.TxnID,
	parentLogLocation LogRecordLocationInfo) TxnEndLogRecord {
	return TxnEndLogRecord{
		lsn:               lsn,
		TransactionID:     TransactionID,
		parentLogLocation: parentLogLocation,
	}
}

type CompensationLogRecord struct {
	lsn               LSN
	TransactionID     txns.TxnID
	parentLogLocation LogRecordLocationInfo
	nextUndoLSN       LSN
	isDelete          bool
	modifiedRecordID  RecordID
	beforeValue       []byte
	afterValue        []byte
}

func (r *CompensationLogRecord) LSN() LSN {
	return r.lsn
}

func NewCompensationLogRecord(
	lsn LSN,
	TransactionID txns.TxnID,
	parentLogLocation LogRecordLocationInfo,
	modifiedRecordID RecordID,
	isDelete bool,
	nextUndoLSN LSN,
	beforeValue []byte,
	afterValue []byte,
) CompensationLogRecord {
	return CompensationLogRecord{
		lsn:               lsn,
		TransactionID:     TransactionID,
		parentLogLocation: parentLogLocation,
		modifiedRecordID:  modifiedRecordID,
		isDelete:          isDelete,
		nextUndoLSN:       nextUndoLSN,
		beforeValue:       beforeValue,
		afterValue:        afterValue,
	}
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
