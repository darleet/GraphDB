package recovery

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
)

type LogRecordType byte

const (
	recordTypeBegin LogRecordType = iota + 1
	recordTypeUpdate
	recordTypeInsert
	recordTypeCommit
	recordTypeAbort
	recordTypeTxnEnd
	recordTypeCompensation
	unreachableType
)

func writeBytes(buf *bytes.Buffer, data []byte) error {
	if err := binary.Write(buf, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	_, err := buf.Write(data)
	return err
}

func readBytes(r *bytes.Reader) ([]byte, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	b := make([]byte, n)
	_, err := r.Read(b)
	return b, err
}

func marshalTxnHeader(buf *bytes.Buffer, typ byte, cur LSN, txnId TransactionID, prev LSN) {
	buf.WriteByte(typ)
	_ = binary.Write(buf, binary.BigEndian, uint64(cur))
	_ = binary.Write(buf, binary.BigEndian, uint64(txnId))
	_ = binary.Write(buf, binary.BigEndian, uint64(prev))
}

func unmarshalTxnHeader(r *bytes.Reader, want byte) (LSN, TransactionID, LSN, error) {
	typ, _ := r.ReadByte()
	if typ != want {
		return NIL_LSN, 0, NIL_LSN, fmt.Errorf("recovery: expected %d got %d", want, typ)
	}
	var cur, txn, prev uint64
	if err := binary.Read(r, binary.BigEndian, &cur); err != nil {
		return NIL_LSN, 0, NIL_LSN, err
	}
	if err := binary.Read(r, binary.BigEndian, &txn); err != nil {
		return NIL_LSN, 0, NIL_LSN, err
	}
	if err := binary.Read(r, binary.BigEndian, &prev); err != nil {
		return NIL_LSN, 0, NIL_LSN, err
	}
	return LSN(cur), TransactionID(txn), LSN(prev), nil
}

func (r BeginLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(recordTypeBegin))
	_ = binary.Write(buf, binary.BigEndian, uint64(r.lsn))
	_ = binary.Write(buf, binary.BigEndian, uint64(r.txnId))
	return buf.Bytes(), nil
}

func (r *BeginLogRecord) UnmarshalBinary(data []byte) error {
	rd := bytes.NewReader(data)
	if typ, _ := rd.ReadByte(); typ != byte(recordTypeBegin) {
		return fmt.Errorf("recovery: expected begin (%d) got %d", recordTypeBegin, typ)
	}
	var tid uint64
	if err := binary.Read(rd, binary.BigEndian, &tid); err != nil {
		return err
	}
	r.txnId = TransactionID(tid)
	return nil
}

func (r UpdateLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	marshalTxnHeader(buf, byte(recordTypeUpdate), r.lsn, r.txnId, r.prevLSN)
	page, err := bufferpool.MarshalPageIdentity(r.pageInfo)
	if err != nil {
		return nil, err
	}
	if err := writeBytes(buf, page); err != nil {
		return nil, err
	}
	_ = binary.Write(buf, binary.BigEndian, int64(r.slotNumber))
	if err := writeBytes(buf, r.beforeValue); err != nil {
		return nil, err
	}
	if err := writeBytes(buf, r.afterValue); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (r *UpdateLogRecord) UnmarshalBinary(data []byte) error {
	rd := bytes.NewReader(data)
	cur, txn, prev, err := unmarshalTxnHeader(rd, byte(recordTypeUpdate))
	if err != nil {
		return err
	}
	r.lsn, r.txnId, r.prevLSN = cur, txn, prev
	pageBytes, err := readBytes(rd)
	if err != nil {
		return err
	}
	if r.pageInfo, err = bufferpool.UnmarshalPageIdentity(pageBytes); err != nil {
		return err
	}
	var slot int64
	if err := binary.Read(rd, binary.BigEndian, &slot); err != nil {
		return err
	}
	r.slotNumber = int(slot)
	if r.beforeValue, err = readBytes(rd); err != nil {
		return err
	}
	if r.afterValue, err = readBytes(rd); err != nil {
		return err
	}
	return nil
}

func (r InsertLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	marshalTxnHeader(buf, byte(recordTypeInsert), r.lsn, r.txnId, r.prevLSN)
	page, err := bufferpool.MarshalPageIdentity(r.pageInfo)
	if err != nil {
		return nil, err
	}
	if err := writeBytes(buf, page); err != nil {
		return nil, err
	}
	_ = binary.Write(buf, binary.BigEndian, int64(r.slotNumber))
	if err := writeBytes(buf, r.value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (r *InsertLogRecord) UnmarshalBinary(data []byte) error {
	rd := bytes.NewReader(data)
	cur, txn, prev, err := unmarshalTxnHeader(rd, byte(recordTypeInsert))
	if err != nil {
		return err
	}
	r.lsn, r.txnId, r.prevLSN = cur, txn, prev
	pageBytes, err := readBytes(rd)
	if err != nil {
		return err
	}
	if r.pageInfo, err = bufferpool.UnmarshalPageIdentity(pageBytes); err != nil {
		return err
	}
	var slot int64
	if err := binary.Read(rd, binary.BigEndian, &slot); err != nil {
		return err
	}
	r.slotNumber = int(slot)
	r.value, err = readBytes(rd)
	return err
}

func marshalHeaderOnly(typ byte, cur LSN, txn TransactionID, prev LSN) ([]byte, error) {
	buf := new(bytes.Buffer)
	marshalTxnHeader(buf, typ, cur, txn, prev)
	return buf.Bytes(), nil
}

func unmarshalHeaderOnly(data []byte, typ byte) (LSN, TransactionID, LSN, error) {
	rd := bytes.NewReader(data)
	return unmarshalTxnHeader(rd, typ)
}

func (r CommitLogRecord) MarshalBinary() ([]byte, error) {
	return marshalHeaderOnly(byte(recordTypeCommit), r.lsn, r.txnId, r.prevLSN)
}
func (r AbortLogRecord) MarshalBinary() ([]byte, error) {
	return marshalHeaderOnly(byte(recordTypeAbort), r.lsn, r.txnId, r.prevLSN)
}
func (r TxnEndLogRecord) MarshalBinary() ([]byte, error) {
	return marshalHeaderOnly(byte(recordTypeTxnEnd), r.lsn, r.txnId, r.prevLSN)
}

func (r *CommitLogRecord) UnmarshalBinary(d []byte) error {
	cur, txn, prev, err := unmarshalHeaderOnly(d, byte(recordTypeCommit))
	if err != nil {
		return err
	}
	r.lsn, r.txnId, r.prevLSN = cur, txn, prev
	return nil
}
func (r *AbortLogRecord) UnmarshalBinary(d []byte) error {
	cur, txn, prev, err := unmarshalHeaderOnly(d, byte(recordTypeAbort))
	if err != nil {
		return err
	}
	r.lsn, r.txnId, r.prevLSN = cur, txn, prev
	return nil
}
func (r *TxnEndLogRecord) UnmarshalBinary(d []byte) error {
	cur, txn, prev, err := unmarshalHeaderOnly(d, byte(recordTypeTxnEnd))
	if err != nil {
		return err
	}
	r.lsn, r.txnId, r.prevLSN = cur, txn, prev
	return nil
}

func (r CompensationLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	marshalTxnHeader(buf, byte(recordTypeCompensation), r.lsn, r.txnId, r.prevLSN)
	_ = binary.Write(buf, binary.BigEndian, uint64(r.nextUndoLSN))
	page, err := bufferpool.MarshalPageIdentity(r.pageInfo)
	if err != nil {
		return nil, err
	}
	if err := writeBytes(buf, page); err != nil {
		return nil, err
	}
	_ = binary.Write(buf, binary.BigEndian, int64(r.slotNumber))
	if err := writeBytes(buf, r.beforeValue); err != nil {
		return nil, err
	}
	if err := writeBytes(buf, r.afterValue); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (r *CompensationLogRecord) UnmarshalBinary(data []byte) error {
	rd := bytes.NewReader(data)
	cur, txn, prev, err := unmarshalTxnHeader(rd, byte(recordTypeCompensation))
	if err != nil {
		return err
	}
	r.lsn, r.txnId, r.prevLSN = cur, txn, prev
	var nextUndo uint64
	if err := binary.Read(rd, binary.BigEndian, &nextUndo); err != nil {
		return err
	}
	r.nextUndoLSN = LSN(nextUndo)
	pageBytes, err := readBytes(rd)
	if err != nil {
		return err
	}
	if r.pageInfo, err = bufferpool.UnmarshalPageIdentity(pageBytes); err != nil {
		return err
	}
	var slot int64
	if err := binary.Read(rd, binary.BigEndian, &slot); err != nil {
		return err
	}
	r.slotNumber = int(slot)
	if r.beforeValue, err = readBytes(rd); err != nil {
		return err
	}
	if r.afterValue, err = readBytes(rd); err != nil {
		return err
	}
	return nil
}

func UnmarshalLogRecord(data []byte) (LogRecordType, any, error) {
	rd := bytes.NewReader(data)
	typ, _ := rd.ReadByte()
	switch LogRecordType(typ) {
	case recordTypeBegin:
		record := &BeginLogRecord{}
		err := record.UnmarshalBinary(data)
		return recordTypeBegin, record, err
	case recordTypeUpdate:
		record := &UpdateLogRecord{}
		err := record.UnmarshalBinary(data)
		return recordTypeUpdate, record, err
	case recordTypeInsert:
		record := &InsertLogRecord{}
		err := record.UnmarshalBinary(data)
		return recordTypeInsert, record, err
	case recordTypeCommit:
		record := &CommitLogRecord{}
		err := record.UnmarshalBinary(data)
		return recordTypeCommit, record, err
	case recordTypeAbort:
		record := &AbortLogRecord{}
		err := record.UnmarshalBinary(data)
		return recordTypeAbort, record, err
	case recordTypeTxnEnd:
		record := &TxnEndLogRecord{}
		err := record.UnmarshalBinary(data)
		return recordTypeTxnEnd, record, err
	case recordTypeCompensation:
		record := &CompensationLogRecord{}
		err := record.UnmarshalBinary(data)
		return recordTypeCommit, record, err
	}

	assert.Assert(typ < byte(unreachableType), "unknown log record type: %d", typ)
	return unreachableType, nil, nil
}
