package recovery

import (
	"bytes"
	"testing"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/transactions"
)

func TestBeginLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewBeginLogRecord(123, transactions.TxnID(456))

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered BeginLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	if original.lsn != recovered.lsn {
		t.Errorf("lsn mismatch: got %v, want %v", recovered.lsn, original.lsn)
	}
	if original.txnId != recovered.txnId {
		t.Errorf("txnId mismatch: got %v, want %v", recovered.txnId, original.txnId)
	}
}

func TestUpdateLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewUpdateLogRecord(
		123,
		transactions.TxnID(456),
		LogRecordLocation{789, 101112, 131415},
		bufferpool.PageIdentity{PageID: 161718, FileID: 192021},
		222324,
		[]byte("before value"),
		[]byte("after value"),
	)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered UpdateLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Compare all fields
	if original.lsn != recovered.lsn {
		t.Errorf("lsn mismatch: got %v, want %v", recovered.lsn, original.lsn)
	}
	if original.txnId != recovered.txnId {
		t.Errorf("txnId mismatch: got %v, want %v", recovered.txnId, original.txnId)
	}
	if original.prevLog != recovered.prevLog {
		t.Errorf("prevLog mismatch: got %v, want %v", recovered.prevLog, original.prevLog)
	}
	if original.pageInfo != recovered.pageInfo {
		t.Errorf("pageInfo mismatch: got %v, want %v", recovered.pageInfo, original.pageInfo)
	}
	if original.slotNumber != recovered.slotNumber {
		t.Errorf("slotNumber mismatch: got %v, want %v", recovered.slotNumber, original.slotNumber)
	}
	if !bytes.Equal(original.beforeValue, recovered.beforeValue) {
		t.Errorf("beforeValue mismatch: got %v, want %v", recovered.beforeValue, original.beforeValue)
	}
	if !bytes.Equal(original.afterValue, recovered.afterValue) {
		t.Errorf("afterValue mismatch: got %v, want %v", recovered.afterValue, original.afterValue)
	}
}

func TestInsertLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewInsertLogRecord(
		123,
		transactions.TxnID(456),
		LogRecordLocation{789, 101112, 131415},
		bufferpool.PageIdentity{PageID: 161718, FileID: 192021},
		222324,
		[]byte("test value"),
	)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered InsertLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Compare all fields
	if original.lsn != recovered.lsn {
		t.Errorf("lsn mismatch: got %v, want %v", recovered.lsn, original.lsn)
	}
	if original.txnId != recovered.txnId {
		t.Errorf("txnId mismatch: got %v, want %v", recovered.txnId, original.txnId)
	}
	if original.prevLog != recovered.prevLog {
		t.Errorf("prevLog mismatch: got %v, want %v", recovered.prevLog, original.prevLog)
	}
	if original.pageInfo != recovered.pageInfo {
		t.Errorf("pageInfo mismatch: got %v, want %v", recovered.pageInfo, original.pageInfo)
	}
	if original.slotNumber != recovered.slotNumber {
		t.Errorf("slotNumber mismatch: got %v, want %v", recovered.slotNumber, original.slotNumber)
	}
	if !bytes.Equal(original.value, recovered.value) {
		t.Errorf("value mismatch: got %v, want %v", recovered.value, original.value)
	}
}

func TestCommitLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewCommitLogRecord(
		123,
		transactions.TxnID(456),
		LogRecordLocation{789, 101112, 131415},
	)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered CommitLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Compare all fields
	if original.lsn != recovered.lsn {
		t.Errorf("lsn mismatch: got %v, want %v", recovered.lsn, original.lsn)
	}
	if original.txnId != recovered.txnId {
		t.Errorf("txnId mismatch: got %v, want %v", recovered.txnId, original.txnId)
	}
	if original.prevLog != recovered.prevLog {
		t.Errorf("prevLog mismatch: got %v, want %v", recovered.prevLog, original.prevLog)
	}
}

func TestAbortLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewAbortLogRecord(
		123,
		transactions.TxnID(456),
		LogRecordLocation{789, 101112, 131415},
	)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered AbortLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Compare all fields
	if original.lsn != recovered.lsn {
		t.Errorf("lsn mismatch: got %v, want %v", recovered.lsn, original.lsn)
	}
	if original.txnId != recovered.txnId {
		t.Errorf("txnId mismatch: got %v, want %v", recovered.txnId, original.txnId)
	}
	if original.prevLog != recovered.prevLog {
		t.Errorf("prevLog mismatch: got %v, want %v", recovered.prevLog, original.prevLog)
	}
}

func TestTxnEndLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewTxnEndLogRecord(
		123,
		transactions.TxnID(456),
		LogRecordLocation{789, 101112, 131415},
	)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered TxnEndLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Compare all fields
	if original.lsn != recovered.lsn {
		t.Errorf("lsn mismatch: got %v, want %v", recovered.lsn, original.lsn)
	}
	if original.txnId != recovered.txnId {
		t.Errorf("txnId mismatch: got %v, want %v", recovered.txnId, original.txnId)
	}
	if original.prevLog != recovered.prevLog {
		t.Errorf("prevLog mismatch: got %v, want %v", recovered.prevLog, original.prevLog)
	}
}

func TestCompensationLogRecord_MarshalUnmarshal(t *testing.T) {
	original := NewCompensationLogRecord(
		123,
		transactions.TxnID(456),
		LogRecordLocation{789, 101112, 131415},
		bufferpool.PageIdentity{PageID: 161718, FileID: 192021},
		222324,
		252627,
		[]byte("before value"),
		[]byte("after value"),
	)

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var recovered CompensationLogRecord
	if err := recovered.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Compare all fields
	if original.lsn != recovered.lsn {
		t.Errorf("lsn mismatch: got %v, want %v", recovered.lsn, original.lsn)
	}
	if original.txnId != recovered.txnId {
		t.Errorf("txnId mismatch: got %v, want %v", recovered.txnId, original.txnId)
	}
	if original.prevLog != recovered.prevLog {
		t.Errorf("prevLog mismatch: got %v, want %v", recovered.prevLog, original.prevLog)
	}
	if original.pageInfo != recovered.pageInfo {
		t.Errorf("pageInfo mismatch: got %v, want %v", recovered.pageInfo, original.pageInfo)
	}
	if original.slotNumber != recovered.slotNumber {
		t.Errorf("slotNumber mismatch: got %v, want %v", recovered.slotNumber, original.slotNumber)
	}
	if original.nextUndoLSN != recovered.nextUndoLSN {
		t.Errorf("nextUndoLSN mismatch: got %v, want %v", recovered.nextUndoLSN, original.nextUndoLSN)
	}
	if !bytes.Equal(original.beforeValue, recovered.beforeValue) {
		t.Errorf("beforeValue mismatch: got %v, want %v", recovered.beforeValue, original.beforeValue)
	}
	if !bytes.Equal(original.afterValue, recovered.afterValue) {
		t.Errorf("afterValue mismatch: got %v, want %v", recovered.afterValue, original.afterValue)
	}
}

func TestInvalidTypeTag(t *testing.T) {
	// Create a valid record first
	original := NewBeginLogRecord(123, transactions.TxnID(456))
	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// Corrupt the type tag
	data[0] = 0xFF

	var recovered BeginLogRecord
	if err := recovered.UnmarshalBinary(data); err == nil {
		t.Fatal("Expected error for invalid type tag, got nil")
	}
}

func TestEmptyData(t *testing.T) {
	var recovered BeginLogRecord
	if err := recovered.UnmarshalBinary([]byte{}); err == nil {
		t.Fatal("Expected error for empty data, got nil")
	}
}

func TestPartialData(t *testing.T) {
	// Create a valid record first
	original := NewBeginLogRecord(123, transactions.TxnID(456))
	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// Truncate the data
	data = data[:len(data)-2]

	var recovered BeginLogRecord
	if err := recovered.UnmarshalBinary(data); err == nil {
		t.Fatal("Expected error for truncated data, got nil")
	}
}