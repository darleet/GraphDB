package recovery

import (
	"fmt"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/txns"
)

type TxnLogChain struct {
	logger        *TxnLogger
	TransactionID txns.TxnID

	lastLocations map[txns.TxnID]common.LogRecordLocationInfo
	err           error
}

func NewTxnLogChain(logger *TxnLogger, TransactionID txns.TxnID) *TxnLogChain {
	return &TxnLogChain{
		logger:        logger,
		TransactionID: TransactionID,

		lastLocations: map[txns.TxnID]common.LogRecordLocationInfo{},
	}
}

func (c *TxnLogChain) SwitchTransactionID(
	TransactionID txns.TxnID,
) *TxnLogChain {
	if c.err != nil {
		return c
	}

	c.TransactionID = TransactionID
	return c
}

func (c *TxnLogChain) Begin() *TxnLogChain {
	if c.err != nil {
		return c
	}

	c.lastLocations[c.TransactionID], c.err = c.logger.AppendBegin(
		c.TransactionID,
	)

	return c
}

func (c *TxnLogChain) Insert(
	recordID common.RecordID,
	value []byte,
) *TxnLogChain {
	if c.err != nil {
		return c
	}

	if _, ok := c.lastLocations[c.TransactionID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.TransactionID)
		return c
	}

	c.lastLocations[c.TransactionID], c.err = c.logger.AppendInsert(
		c.TransactionID,
		c.lastLocations[c.TransactionID],
		recordID,
		value,
	)

	return c
}

func (c *TxnLogChain) Update(
	recordID common.RecordID,
	beforeValue, afterValue []byte,
) *TxnLogChain {
	if c.err != nil {
		return c
	}

	if _, ok := c.lastLocations[c.TransactionID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.TransactionID)
		return c
	}

	c.lastLocations[c.TransactionID], c.err = c.logger.AppendUpdate(
		c.TransactionID,
		c.lastLocations[c.TransactionID],
		recordID,
		beforeValue,
		afterValue,
	)

	return c
}

func (c *TxnLogChain) Delete(
	recordID common.RecordID,
) *TxnLogChain {
	if c.err != nil {
		return c
	}

	if _, ok := c.lastLocations[c.TransactionID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.TransactionID)
		return c
	}

	c.lastLocations[c.TransactionID], c.err = c.logger.AppendDelete(
		c.TransactionID,
		c.lastLocations[c.TransactionID],
		recordID,
	)

	return c
}

func (c *TxnLogChain) Commit() *TxnLogChain {
	if c.err != nil {
		return c
	}

	if _, ok := c.lastLocations[c.TransactionID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.TransactionID)
		return c
	}

	c.lastLocations[c.TransactionID], c.err = c.logger.AppendCommit(
		c.TransactionID,
		c.lastLocations[c.TransactionID],
	)

	return c
}

func (c *TxnLogChain) Abort() *TxnLogChain {
	if c.err != nil {
		return c
	}

	if _, ok := c.lastLocations[c.TransactionID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.TransactionID)
		return c
	}

	c.lastLocations[c.TransactionID], c.err = c.logger.AppendAbort(
		c.TransactionID,
		c.lastLocations[c.TransactionID],
	)

	return c
}

func (c *TxnLogChain) TxnEnd() *TxnLogChain {
	if c.err != nil {
		return c
	}

	if _, ok := c.lastLocations[c.TransactionID]; !ok {
		c.err = fmt.Errorf("no last location found for %d", c.TransactionID)
		return c
	}

	c.lastLocations[c.TransactionID], c.err = c.logger.AppendTxnEnd(
		c.TransactionID,
		c.lastLocations[c.TransactionID],
	)

	return c
}

func (c *TxnLogChain) CheckpointBegin() *TxnLogChain {
	if c.err != nil {
		return c
	}

	_, c.err = c.logger.AppendCheckpointBegin()

	return c
}

func (c *TxnLogChain) CheckpointEnd(
	ATT []txns.TxnID,
	DPT map[common.PageIdentity]common.LogRecordLocationInfo,
) *TxnLogChain {
	if c.err != nil {
		return c
	}

	_, c.err = c.logger.AppendCheckpointEnd(ATT, DPT)

	return c
}

func (c *TxnLogChain) Loc() common.LogRecordLocationInfo {
	return c.lastLocations[c.TransactionID]
}

func (c *TxnLogChain) Err() error {
	return c.err
}
