package recovery

type LogFile struct {
	buffer []byte
}

type TransactionID uint64

type LSN uint64

var NIL_LSN LSN = LSN(^(uint64(0)))

type BeginLogRecord struct {
	txnId TransactionID
}

func (b *BeginLogRecord) AppendBinary(b []byte) ([]byte, error) {
	b = append()

}

func (l *LogFile) WriteBegin(txnId TransactionID) {
	BeginLogRecord
	l.buffer.
}

func (l *LogFile) WriteUpdate(txnId TransactionID, prevLSN LSN) {}

func (l *LogFile) WriteInsert(txnId TransactionID, prevLSN LSN) {}

func (l *LogFile) WriteCommit(txnId TransactionID, prevLSN LSN) {}

func (l *LogFile) WriteAbort(txnId TransactionID, prevLSN LSN) {}

func (l *LogFile) WriteTxnEnd(txnId TransactionID, prevLSN LSN) {}

func (l *LogFile) WriteCLR(txnId TransactionID, prevLSN LSN) {}
