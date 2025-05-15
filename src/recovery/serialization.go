package recovery

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/transactions"
)

// Type tags for each log record type.
const (
	TypeBegin byte = iota + 1
	TypeUpdate
	TypeInsert
	TypeCommit
	TypeAbort
	TypeTxnEnd
	TypeCompensation
	TypeCheckpointBegin
	TypeCheckpointEnd
)

func (l *LogRecordLocation) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, l.Lsn); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, l.PageID); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, l.SlotID); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (l *LogRecordLocation) UnmarshalBinary(data []byte) error {
	rd := bytes.NewReader(data)
	if err := binary.Read(rd, binary.BigEndian, &l.Lsn); err != nil {
		return err
	}
	if err := binary.Read(rd, binary.BigEndian, &l.PageID); err != nil {
		return err
	}
	if err := binary.Read(rd, binary.BigEndian, &l.SlotID); err != nil {
		return err
	}
	return nil
}

// MarshalBinary for BeginLogRecord.
func (b *BeginLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(TypeBegin)
	if err := binary.Write(buf, binary.BigEndian, b.lsn); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, b.txnId); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (b *BeginLogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 1 {
		return errors.New("insufficient data for type tag")
	}
	if data[0] != TypeBegin {
		return fmt.Errorf("invalid type tag for BeginLogRecord: %x", data[0])
	}
	reader := bytes.NewReader(data[1:])
	if err := binary.Read(reader, binary.BigEndian, &b.lsn); err != nil {
		return err
	}
	return binary.Read(reader, binary.BigEndian, &b.txnId)
}

// MarshalBinary for UpdateLogRecord.
func (u *UpdateLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(TypeUpdate)
	if err := binary.Write(buf, binary.BigEndian, u.lsn); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, u.txnId); err != nil {
		return nil, err
	}
	locationBytes, err := u.prevLog.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(locationBytes)
	pageData, err := u.pageInfo.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(pageData)
	if err := binary.Write(buf, binary.BigEndian, u.slotNumber); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(u.beforeValue))); err != nil {
		return nil, err
	}
	buf.Write(u.beforeValue)
	if err := binary.Write(buf, binary.BigEndian, uint32(len(u.afterValue))); err != nil {
		return nil, err
	}
	buf.Write(u.afterValue)
	return buf.Bytes(), nil
}

func (u *UpdateLogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 1 {
		return errors.New("insufficient data")
	}
	if data[0] != TypeUpdate {
		return fmt.Errorf("invalid type tag for UpdateLogRecord: %x", data[0])
	}
	reader := bytes.NewReader(data[1:])

	if err := binary.Read(reader, binary.BigEndian, &u.lsn); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &u.txnId); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &u.prevLog); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &u.pageInfo); err != nil {
		return err
	}
	var slotNum uint32
	if err := binary.Read(reader, binary.BigEndian, &slotNum); err != nil {
		return err
	}
	u.slotNumber = slotNum

	var beforeLen uint32
	if err := binary.Read(reader, binary.BigEndian, &beforeLen); err != nil {
		return err
	}
	u.beforeValue = make([]byte, beforeLen)
	if _, err := io.ReadFull(reader, u.beforeValue); err != nil {
		return err
	}

	var afterLen uint32
	if err := binary.Read(reader, binary.BigEndian, &afterLen); err != nil {
		return err
	}
	u.afterValue = make([]byte, afterLen)
	_, err := io.ReadFull(reader, u.afterValue)
	return err
}

// MarshalBinary for InsertLogRecord.
func (i *InsertLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(TypeInsert)
	if err := binary.Write(buf, binary.BigEndian, i.lsn); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, i.txnId); err != nil {
		return nil, err
	}
	d, err := i.prevLog.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(d)

	pageData, err := i.pageInfo.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(pageData)
	if err := binary.Write(buf, binary.BigEndian, i.slotNumber); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(i.value))); err != nil {
		return nil, err
	}
	buf.Write(i.value)
	return buf.Bytes(), nil
}

func (i *InsertLogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 1 || data[0] != TypeInsert {
		return errors.New("invalid type tag for InsertLogRecord")
	}
	reader := bytes.NewReader(data[1:])

	if err := binary.Read(reader, binary.BigEndian, &i.lsn); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &i.txnId); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &i.prevLog); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &i.pageInfo); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &i.slotNumber); err != nil {
		return err
	}
	var valueLen uint32
	if err := binary.Read(reader, binary.BigEndian, &valueLen); err != nil {
		return err
	}
	i.value = make([]byte, valueLen)
	_, err := io.ReadFull(reader, i.value)
	return err
}

// MarshalBinary for CommitLogRecord.
func (c *CommitLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(TypeCommit)
	if err := binary.Write(buf, binary.BigEndian, c.lsn); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, c.txnId); err != nil {
		return nil, err
	}
	d, err := c.prevLog.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(d)
	return buf.Bytes(), nil
}

func (c *CommitLogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 1 || data[0] != TypeCommit {
		return errors.New("invalid type tag for CommitLogRecord")
	}
	reader := bytes.NewReader(data[1:])

	if err := binary.Read(reader, binary.BigEndian, &c.lsn); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &c.txnId); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &c.prevLog); err != nil {
		return err
	}
	return nil
}

// MarshalBinary for AbortLogRecord.
func (a *AbortLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(TypeAbort)
	if err := binary.Write(buf, binary.BigEndian, a.lsn); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, a.txnId); err != nil {
		return nil, err
	}
	d, err := a.prevLog.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(d)
	return buf.Bytes(), nil
}

func (a *AbortLogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 1 || data[0] != TypeAbort {
		return errors.New("invalid type tag for AbortLogRecord")
	}
	reader := bytes.NewReader(data[1:])

	if err := binary.Read(reader, binary.BigEndian, &a.lsn); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &a.txnId); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &a.prevLog); err != nil {
		return err
	}
	return nil
}

// MarshalBinary for TxnEndLogRecord.
func (t *TxnEndLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(TypeTxnEnd)
	if err := binary.Write(buf, binary.BigEndian, t.lsn); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, t.txnId); err != nil {
		return nil, err
	}
	d, err := t.prevLog.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(d)
	return buf.Bytes(), nil
}

func (t *TxnEndLogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 1 || data[0] != TypeTxnEnd {
		return errors.New("invalid type tag for TxnEndLogRecord")
	}
	reader := bytes.NewReader(data[1:])

	if err := binary.Read(reader, binary.BigEndian, &t.lsn); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &t.txnId); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &t.prevLog); err != nil {
		return err
	}
	return nil
}

// MarshalBinary for CompensationLogRecord.
func (c *CompensationLogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(TypeCompensation)
	if err := binary.Write(buf, binary.BigEndian, c.lsn); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, c.txnId); err != nil {
		return nil, err
	}
	d, err := c.prevLog.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(d)
	if err := binary.Write(buf, binary.BigEndian, c.nextUndoLSN); err != nil {
		return nil, err
	}
	pageData, err := c.pageInfo.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(pageData)
	if err := binary.Write(buf, binary.BigEndian, (c.slotNumber)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(c.beforeValue))); err != nil {
		return nil, err
	}
	buf.Write(c.beforeValue)
	if err := binary.Write(buf, binary.BigEndian, uint32(len(c.afterValue))); err != nil {
		return nil, err
	}
	buf.Write(c.afterValue)
	return buf.Bytes(), nil
}

func (c *CompensationLogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 1 || data[0] != TypeCompensation {
		return errors.New("invalid type tag for CompensationLogRecord")
	}
	reader := bytes.NewReader(data[1:])

	if err := binary.Read(reader, binary.BigEndian, &c.lsn); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &c.txnId); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &c.prevLog); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &c.nextUndoLSN); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &c.pageInfo); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &c.slotNumber); err != nil {
		return err
	}

	var beforeLen uint32
	if err := binary.Read(reader, binary.BigEndian, &beforeLen); err != nil {
		return err
	}
	c.beforeValue = make([]byte, beforeLen)
	if _, err := io.ReadFull(reader, c.beforeValue); err != nil {
		return err
	}

	var afterLen uint32
	if err := binary.Read(reader, binary.BigEndian, &afterLen); err != nil {
		return err
	}
	c.afterValue = make([]byte, afterLen)
	_, err := io.ReadFull(reader, c.afterValue)
	return err
}

func (c *CheckpointBegin) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(TypeCheckpointBegin)
	if err := binary.Write(buf, binary.BigEndian, c.lsn); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *CheckpointBegin) UnmarshalBinary(data []byte) error {
	if len(data) < 1 || data[0] != TypeCheckpointBegin {
		return errors.New("invalid type tag for CompensationLogRecord")
	}
	reader := bytes.NewReader(data[1:])
	if err := binary.Read(reader, binary.BigEndian, &c.lsn); err != nil {
		return err
	}
	return nil
}

func (c *CheckpointEnd) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(TypeCheckpointEnd)

	// Write LSN
	if err := binary.Write(buf, binary.BigEndian, c.lsn); err != nil {
		return nil, err
	}

	// Write active transactions
	if err := binary.Write(buf, binary.BigEndian, uint32(len(c.activeTransactions))); err != nil {
		return nil, err
	}
	for _, txnID := range c.activeTransactions {
		if err := binary.Write(buf, binary.BigEndian, txnID); err != nil {
			return nil, err
		}
	}

	// Write dirty page table
	if err := binary.Write(buf, binary.BigEndian, uint32(len(c.dirtyPageTable))); err != nil {
		return nil, err
	}
	for pageID, pageLSN := range c.dirtyPageTable {
		pageData, err := pageID.MarshalBinary()
		if err != nil {
			return nil, err
		}
		// Write page identity length and data
		if err := binary.Write(buf, binary.BigEndian, uint32(len(pageData))); err != nil {
			return nil, err
		}
		buf.Write(pageData)
		// Write associated LSN
		if err := binary.Write(buf, binary.BigEndian, pageLSN); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (c *CheckpointEnd) UnmarshalBinary(data []byte) error {
	if len(data) < 1 {
		return errors.New("insufficient data for type tag")
	}
	if data[0] != TypeCheckpointEnd {
		return errors.New("invalid type tag for CheckpointEnd")
	}

	reader := bytes.NewReader(data[1:])

	// Read LSN
	if err := binary.Read(reader, binary.BigEndian, &c.lsn); err != nil {
		return err
	}

	// Read active transactions
	var activeTxnsLen uint32
	if err := binary.Read(reader, binary.BigEndian, &activeTxnsLen); err != nil {
		return err
	}
	c.activeTransactions = make([]transactions.TxnID, activeTxnsLen)
	for i := range c.activeTransactions {
		if err := binary.Read(reader, binary.BigEndian, &c.activeTransactions[i]); err != nil {
			return err
		}
	}

	// Read dirty page table
	var dirtyPagesLen uint32
	if err := binary.Read(reader, binary.BigEndian, &dirtyPagesLen); err != nil {
		return err
	}
	c.dirtyPageTable = make(map[bufferpool.PageIdentity]LSN, dirtyPagesLen)
	for i := 0; i < int(dirtyPagesLen); i++ {
		// Read page identity
		var pageDataLen uint32
		if err := binary.Read(reader, binary.BigEndian, &pageDataLen); err != nil {
			return err
		}
		pageData := make([]byte, pageDataLen)
		if _, err := io.ReadFull(reader, pageData); err != nil {
			return err
		}
		var pageID bufferpool.PageIdentity
		if err := pageID.UnmarshalBinary(pageData); err != nil {
			return err
		}

		// Read associated LSN
		var pageLSN LSN
		if err := binary.Read(reader, binary.BigEndian, &pageLSN); err != nil {
			return err
		}

		c.dirtyPageTable[pageID] = pageLSN
	}

	return nil
}
