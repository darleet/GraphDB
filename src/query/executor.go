package query

import (
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
)

type StorageEngine interface {
	NewQueue(common.TxnID) (Queue, error)
	NewAggregationAssociativeArray(common.TxnID) (AssociativeArray[storage.VertexID, float64], error)
	NewBitMap(common.TxnID) (Visited, error)
	Neighbors(t common.TxnID, v storage.VertexID) (NeighborIter, error)
	GetVertexRID(t common.TxnID, v storage.VertexID) (VertexIDWithRID, error)
	AllVerticesWithValue(t common.TxnID, field string, value []byte) (VerticesIter, error)
	CountOfFilteredEdges(t common.TxnID, v storage.VertexID, f EdgeFilter) (uint64, error)
	GetAllVertices(t common.TxnID) (VerticesIter, error)
	GetNeighborsWithEdgeFilter(t common.TxnID, v storage.VertexID, filter EdgeFilter) (VerticesIter, error)
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
