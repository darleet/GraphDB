package recovery

import (
	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/transactions"
)

type LogChain struct {
	logger *TxnLogger
	loc    LogRecordLocationInfo
	err    error
}

func NewLogChain(logger *TxnLogger) *LogChain {
	return &LogChain{
		logger: logger,
		loc:    NewNilLogRecordLocation(),
	}
}

func (c *LogChain) Begin(txnId transactions.TxnID) *LogChain {
	if c.err != nil {
		return c
	}
	c.loc, c.err = c.logger.AppendBegin(txnId)
	return c
}

func (c *LogChain) Insert(txnId transactions.TxnID, pageInfo bufferpool.PageIdentity, slotNumber uint32, value []byte) *LogChain {
	if c.err != nil {
		return c
	}
	c.loc, c.err = c.logger.AppendInsert(txnId, c.loc, pageInfo, slotNumber, value)
	return c
}

func (c *LogChain) Update(txnId transactions.TxnID, pageInfo bufferpool.PageIdentity, slotNumber uint32, beforeValue, afterValue []byte) *LogChain {
	if c.err != nil {
		return c
	}
	c.loc, c.err = c.logger.AppendUpdate(txnId, c.loc, pageInfo, slotNumber, beforeValue, afterValue)
	return c
}

func (c *LogChain) Commit(txnId transactions.TxnID) *LogChain {
	if c.err != nil {
		return c
	}
	c.loc, c.err = c.logger.AppendCommit(txnId, c.loc)
	return c
}

func (c *LogChain) Abort(txnId transactions.TxnID) *LogChain {
	if c.err != nil {
		return c
	}
	c.loc, c.err = c.logger.AppendAbort(txnId, c.loc)
	return c
}

func (c *LogChain) TxnEnd(txnId transactions.TxnID) *LogChain {
	if c.err != nil {
		return c
	}
	c.loc, c.err = c.logger.AppendTxnEnd(txnId, c.loc)
	return c
}

func (c *LogChain) CheckpointBegin() *LogChain {
	if c.err != nil {
		return c
	}
	c.loc, c.err = c.logger.AppendCheckpointBegin()
	return c
}

func (c *LogChain) CheckpointEnd(ATT []transactions.TxnID, DPT map[bufferpool.PageIdentity]LogRecordLocationInfo) *LogChain {
	if c.err != nil {
		return c
	}
	c.loc, c.err = c.logger.AppendCheckpointEnd(ATT, DPT)
	return c
}

func (c *LogChain) Loc() LogRecordLocationInfo {
	return c.loc
}

func (c *LogChain) Err() error {
	return c.err
}
