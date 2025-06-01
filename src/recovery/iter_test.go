package recovery

import (
	"testing"
)

func TestIter(t *testing.T) {
	// pool := bufferpool.NewBufferPoolMock()
	//
	// logger := &TxnLogger{
	// 	pool:            pool,
	// 	mu:              sync.Mutex{},
	// 	logRecordsCount: 0,
	// 	logfileID:       0,
	// 	lastLogLocation: LogRecordLocationInfo{
	// 		Lsn: 0,
	// 		Location: FileLocation{
	// 			PageID:  0,
	// 			SlotNum: 0,
	// 		},
	// 	},
	// 	getActiveTransactions: func() []txns.TxnID {
	// 		panic("TODO")
	// 	},
	// }
	//
	// txnId := txns.TxnID(1)
	// dataPageId := bufferpool.PageIdentity{
	// 	FileID: logger.logfileID,
	// 	PageID: logger.lastLogLocation.Location.PageID,
	// }
	// 
	// chain := NewTxnLogChain(logger)
}
