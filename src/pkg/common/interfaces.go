package common

type ITxnLoggerWithContext interface {
	AppendBegin() error
	AppendDelete(recordID RecordID) (LogRecordLocInfo, error)
	AppendInsert(recordID RecordID, value []byte) (LogRecordLocInfo, error)
	AppendUpdate(
		recordID RecordID,
		before []byte,
		after []byte,
	) (LogRecordLocInfo, error)
	AppendCommit() error
	AppendAbort() error
	AppendTxnEnd() error
	Rollback()
}
