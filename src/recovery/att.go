package recovery

import "github.com/Blackdeer1524/GraphDB/src/transactions"

type txnStatus byte

const (
	TxnStatusUndo txnStatus = iota
	TxnStatusCommit
)

type ATTEntry struct {
	status   txnStatus
	location LogRecordLocation
}

func NewATTEntry(
	status txnStatus,
	location LogRecordLocation,
) ATTEntry {
	return ATTEntry{
		status:   status,
		location: location,
	}
}

type ActiveTransactionsTable struct {
	table map[transactions.TxnID]ATTEntry
}

func NewATT() ActiveTransactionsTable {
	return ActiveTransactionsTable{
		table: map[transactions.TxnID]ATTEntry{},
	}
}

// returns true iff it is the first record for the transaction
func (att *ActiveTransactionsTable) Insert(
	id transactions.TxnID,
	tag LogRecordTypeTag,
	entry ATTEntry,
) bool {
	if tag == TypeTxnEnd {
		delete(att.table, id)
		return false
	}

	prevEntry, alreadyExists := att.table[id]
	if !alreadyExists {
		att.table[id] = entry
		return true
	}
	// https://stackoverflow.com/questions/42605337/cannot-assign-to-struct-field-in-a-map
	if prevEntry.status == TxnStatusUndo {
		prevEntry.status = entry.status
	}
	if !entry.location.isNil() {
		prevEntry.location = entry.location
	}
	att.table[id] = prevEntry
	return false
}
