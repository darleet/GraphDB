package query

type StorageEngine interface {
	NewQueue(TxnID) (Queue, error)
	NewAggregationAssociativeArray(TxnID) (AssociativeArray[VertexID, float64], error)
	NewBitMap(TxnID) (Visited, error)
	Neighbors(t TxnID, v VertexID) (NeighborIter, error)
	GetVertexRID(t TxnID, v VertexID) (VertexIDWithRID, error)
	AllVerticesWithValue(t TxnID, field string, value []byte) (VerticesIter, error)
	CountOfFilteredEdges(t TxnID, v VertexID, f EdgeFilter) (uint64, error)
	GetAllVertices(t TxnID) (VerticesIter, error)
	GetNeighborsWithEdgeFilter(t TxnID, v VertexID, filter EdgeFilter) (VerticesIter, error)
}

type Executor struct {
	se StorageEngine
	tm TransactionManager
}

func New(se StorageEngine) *Executor {
	return &Executor{
		se: se,
	}
}
