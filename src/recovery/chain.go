package recovery

import (
	"fmt"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	txns "github.com/Blackdeer1524/GraphDB/src/transactions"
)

type TxnLogChain struct {
	logger *TxnLogger
	txnId  txns.TxnID

	lastLocations map[txns.TxnID]LogRecordLocationInfo
	err           error
}

func NewTxnLogChain(logger *TxnLogger, txnId txns.TxnID) *TxnLogChain {
	return &TxnLogChain{
		logger: logger,
		txnId:  txnId,

		lastLocations: map[txns.TxnID]LogRecordLocationInfo{},
	}
}

func (c *TxnLogChain) SwitchTxnId(txnId txns.TxnID) *TxnLogChain {
	if c.err != nil {
		return c
	}
	c.txnId = txnId
	return c
}

func (c *TxnLogChain) Begin() *TxnLogChain {
	if c.err != nil {
		return c
	}
	c.lastLocations[c.txnId], c.err = c.logger.AppendBegin(c.txnId)
	return c
}

func (c *TxnLogChain) Insert(pageInfo bufferpool.PageIdentity, slotNumber uint32, value []byte) *TxnLogChain {
	if c.err != nil {
		return c
	}
	if _, ok := c.lastLocations[c.txnId]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.txnId)
		return c
	}

	c.lastLocations[c.txnId], c.err = c.logger.AppendInsert(c.txnId, c.lastLocations[c.txnId], pageInfo, slotNumber, value)
	return c
}

func (c *TxnLogChain) Update(pageInfo bufferpool.PageIdentity, slotNumber uint32, beforeValue, afterValue []byte) *TxnLogChain {
	if c.err != nil {
		return c
	}
	if _, ok := c.lastLocations[c.txnId]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.txnId)
		return c
	}

	c.lastLocations[c.txnId], c.err = c.logger.AppendUpdate(c.txnId, c.lastLocations[c.txnId], pageInfo, slotNumber, beforeValue, afterValue)
	return c
}

func (c *TxnLogChain) Commit() *TxnLogChain {
	if c.err != nil {
		return c
	}
	if _, ok := c.lastLocations[c.txnId]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.txnId)
		return c
	}

	c.lastLocations[c.txnId], c.err = c.logger.AppendCommit(c.txnId, c.lastLocations[c.txnId])
	return c
}

func (c *TxnLogChain) Abort() *TxnLogChain {
	if c.err != nil {
		return c
	}
	if _, ok := c.lastLocations[c.txnId]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.txnId)
		return c
	}

	c.lastLocations[c.txnId], c.err = c.logger.AppendAbort(c.txnId, c.lastLocations[c.txnId])
	return c
}

func (c *TxnLogChain) TxnEnd() *TxnLogChain {
	if c.err != nil {
		return c
	}
	if _, ok := c.lastLocations[c.txnId]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.txnId)
		return c
	}

	c.lastLocations[c.txnId], c.err = c.logger.AppendTxnEnd(c.txnId, c.lastLocations[c.txnId])
	return c
}

func (c *TxnLogChain) CheckpointBegin() *TxnLogChain {
	if c.err != nil {
		return c
	}
	_, c.err = c.logger.AppendCheckpointBegin()
	return c
}

func (c *TxnLogChain) CheckpointEnd(ATT []txns.TxnID, DPT map[bufferpool.PageIdentity]LogRecordLocationInfo) *TxnLogChain {
	if c.err != nil {
		return c
	}
	_, c.err = c.logger.AppendCheckpointEnd(ATT, DPT)
	return c
}

func (c *TxnLogChain) Loc() LogRecordLocationInfo {
	return c.lastLocations[c.txnId]
}

func (c *TxnLogChain) Err() error {
	return c.err
}
