package query

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Tests for GetVertexesOnDepth
func TestGetVertexesOnDepth_NilStorageEngine(t *testing.T) {
	e := &Executor{se: nil, tm: &mockTransactionManager{}}

	_, err := e.GetVertexesOnDepth(1, 0)
	require.Error(t, err)
	assert.Equal(t, "storage engine is nil", err.Error())
}

func TestGetVertexesOnDepth_TransactionBeginError(t *testing.T) {
	se := newDataMockStorageEngine(nil, nil, nil, nil, nil, nil)
	tm := &mockTransactionManager{beginErr: errors.New("begin error")}
	e := &Executor{se: se, tm: tm}

	_, err := e.GetVertexesOnDepth(1, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to begin transaction: begin error")
}

func TestGetVertexesOnDepth_GetVertexRIDError(t *testing.T) {
	se := newDataMockStorageEngine(nil, nil, nil, nil, nil, errors.New("rid error"))
	tm := &mockTransactionManager{nextTxnID: 1}
	e := &Executor{se: se, tm: tm}

	_, err := e.GetVertexesOnDepth(1, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get start vertex: rid error")
}

func TestGetVertexesOnDepth_Depth0(t *testing.T) {
	vertices := []VertexID{1}
	edges := [][]VertexID{}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)
	tm := &mockTransactionManager{nextTxnID: 1}
	e := &Executor{se: se, tm: tm}

	expected := []VertexIDWithRID{{V: 1, R: RID{PageID: 100}}}
	res, err := e.GetVertexesOnDepth(1, 0)
	require.NoError(t, err)
	assert.Equal(t, expected, res)
}

func TestGetVertexesOnDepth_Depth1(t *testing.T) {
	vertices := []VertexID{1, 2, 3}
	edges := [][]VertexID{{1, 2}, {1, 3}}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)
	tm := &mockTransactionManager{nextTxnID: 1}
	e := &Executor{se: se, tm: tm}

	expected := []VertexIDWithRID{
		{V: 2, R: RID{PageID: 200}},
		{V: 3, R: RID{PageID: 300}},
	}
	res, err := e.GetVertexesOnDepth(1, 1)
	require.NoError(t, err)
	assert.ElementsMatch(t, expected, res)
}

func TestGetVertexesOnDepth_Depth2(t *testing.T) {
	vertices := []VertexID{1, 2, 3, 4, 5}
	edges := [][]VertexID{{1, 2}, {1, 3}, {2, 4}, {3, 5}}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)
	tm := &mockTransactionManager{nextTxnID: 1}
	e := &Executor{se: se, tm: tm}

	expected := []VertexIDWithRID{
		{V: 4, R: RID{PageID: 400}},
		{V: 5, R: RID{PageID: 500}},
	}
	res, err := e.GetVertexesOnDepth(1, 2)
	require.NoError(t, err)
	assert.ElementsMatch(t, expected, res)
}

func TestGetVertexesOnDepth_WithCycle(t *testing.T) {
	vertices := []VertexID{1, 2, 3}
	edges := [][]VertexID{{1, 2}, {2, 3}, {3, 1}}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)
	tm := &mockTransactionManager{nextTxnID: 1}
	e := &Executor{se: se, tm: tm}

	expected := []VertexIDWithRID{{V: 2, R: RID{PageID: 200}}, {V: 3, R: RID{PageID: 300}}}
	res, err := e.GetVertexesOnDepth(1, 1)
	require.NoError(t, err)
	assert.Equal(t, expected, res)
}

func TestGetVertexesOnDepth_CommitError(t *testing.T) {
	vertices := []VertexID{1}
	edges := [][]VertexID{}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)
	tm := &mockTransactionManager{nextTxnID: 1, commitErr: errors.New("commit error")}
	e := &Executor{se: se, tm: tm}

	_, err := e.GetVertexesOnDepth(1, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to commit transaction: commit error")
}

func TestGetVertexesOnDepth_RollbackOnError(t *testing.T) {
	vertices := []VertexID{1}
	edges := [][]VertexID{}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, errors.New("rid error"))
	tm := &mockTransactionManager{nextTxnID: 1}
	e := &Executor{se: se, tm: tm}

	_, err := e.GetVertexesOnDepth(1, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get start vertex: rid error")
}

func TestGetVertexesOnDepth_DepthOverflow(t *testing.T) {
	vertices := []VertexID{1}
	edges := [][]VertexID{}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)
	tm := &mockTransactionManager{nextTxnID: 1}
	e := &Executor{se: se, tm: tm}

	res, err := e.GetVertexesOnDepth(1, ^uint32(0))
	require.NoError(t, err)
	assert.Empty(t, res, "При максимальной глубине ожидается пустой результат")
}

func TestBFS_NewQueueError(t *testing.T) {
	vertices := []VertexID{1}
	edges := [][]VertexID{}
	se := newDataMockStorageEngine(vertices, edges, nil, errors.New("queue error"), nil, nil)
	e := &Executor{se: se}
	start := VertexIDWithRID{V: 1, R: RID{PageID: 100}}

	_, err := e.bfsWithDepth(0, start, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "queue error")
}

func TestBFS_NewBitMapError(t *testing.T) {
	vertices := []VertexID{1}
	edges := [][]VertexID{}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, errors.New("bitmap error"), nil)
	e := &Executor{se: se}
	start := VertexIDWithRID{V: 1, R: RID{PageID: 100}}

	_, err := e.bfsWithDepth(0, start, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bitmap error")
}

func TestBFS_Depth0(t *testing.T) {
	vertices := []VertexID{1}
	edges := [][]VertexID{}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)
	e := &Executor{se: se}
	start := VertexIDWithRID{V: 1, R: RID{PageID: 100}}

	res, err := e.bfsWithDepth(0, start, 0)
	require.NoError(t, err)
	assert.Equal(t, []VertexIDWithRID{start}, res)
}

func TestBFS_Depth1(t *testing.T) {
	vertices := []VertexID{1, 2, 3}
	edges := [][]VertexID{{1, 2}, {1, 3}}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)
	e := &Executor{se: se}
	start := VertexIDWithRID{V: 1, R: RID{PageID: 100}}

	res, err := e.bfsWithDepth(0, start, 1)
	require.NoError(t, err)
	assert.ElementsMatch(t, []VertexIDWithRID{
		{V: 2, R: RID{PageID: 200}},
		{V: 3, R: RID{PageID: 300}},
	}, res)
}

func TestBFS_Depth2(t *testing.T) {
	vertices := []VertexID{1, 2, 3, 4, 5}
	edges := [][]VertexID{{1, 2}, {1, 3}, {2, 4}, {3, 5}}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)
	e := &Executor{se: se}
	start := VertexIDWithRID{V: 1, R: RID{PageID: 100}}

	res, err := e.bfsWithDepth(0, start, 2)
	require.NoError(t, err)
	assert.ElementsMatch(t, []VertexIDWithRID{
		{V: 4, R: RID{PageID: 400}},
		{V: 5, R: RID{PageID: 500}},
	}, res)
}

func TestBFS_WithCycle(t *testing.T) {
	vertices := []VertexID{1, 2, 3}
	edges := [][]VertexID{{1, 2}, {2, 3}, {3, 1}}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)
	e := &Executor{se: se}
	start := VertexIDWithRID{V: 1, R: RID{PageID: 100}}

	res, err := e.bfsWithDepth(0, start, 1)
	require.NoError(t, err)
	assert.Equal(t, []VertexIDWithRID{{V: 2, R: RID{PageID: 200}}, {V: 3, R: RID{PageID: 300}}}, res)
}

func TestBFS_TraverseNeighborsError(t *testing.T) {
	vertices := []VertexID{1}
	edges := [][]VertexID{}
	se := newDataMockStorageEngine(vertices, edges, errors.New("neighbors error"), nil, nil, nil)
	e := &Executor{se: se}
	start := VertexIDWithRID{V: 1, R: RID{PageID: 100}}

	_, err := e.bfsWithDepth(0, start, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to traverse neighbors: neighbors error")
}

func TestBFS_NoVerticesAtTargetDepth(t *testing.T) {
	vertices := []VertexID{1}
	edges := [][]VertexID{}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)
	e := &Executor{se: se}
	start := VertexIDWithRID{V: 1, R: RID{PageID: 100}}

	res, err := e.bfsWithDepth(0, start, 1)
	require.NoError(t, err)
	assert.Empty(t, res)
}

func TestBFS_MultiplePathsToSameVertex(t *testing.T) {
	vertices := []VertexID{1, 2, 3, 4}
	edges := [][]VertexID{{1, 2}, {1, 3}, {2, 4}, {3, 4}}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)
	e := &Executor{se: se}
	start := VertexIDWithRID{V: 1, R: RID{PageID: 100}}

	res, err := e.bfsWithDepth(0, start, 2)
	require.NoError(t, err)
	assert.Equal(t, []VertexIDWithRID{{V: 4, R: RID{PageID: 400}}}, res)
}

// GetAllVertexesWithFieldValue

func TestGetAllVertexesWithFieldValue(t *testing.T) {
	t.Run("nil storage engine", func(t *testing.T) {
		exec := &Executor{}
		res, err := exec.GetAllVertexesWithFieldValue("f", []byte("v"))
		assert.Nil(t, res)
		assert.Error(t, err)
	})

	t.Run("begin tx fails", func(t *testing.T) {
		tm := new(mockTxnManager)
		tm.On("Begin").Return(TxnID(0), errors.New("begin failed"))

		exec := &Executor{tm: tm, se: new(DataMockStorageEngine)}

		res, err := exec.GetAllVertexesWithFieldValue("f", []byte("v"))
		assert.Nil(t, res)
		assert.ErrorContains(t, err, "failed to begin transaction")
	})

	t.Run("all vertices fails", func(t *testing.T) {
		tm := new(mockTxnManager)
		se := new(MockStorageEngine)

		tm.On("Begin").Return(TxnID(1), nil)
		tm.On("RollbackTx", TxnID(1)).Return(nil)
		se.On("AllVerticesWithValue", TxnID(1), "f", []byte("v")).
			Return(new(mockAllVerticesIter), errors.New("storage fail"))

		exec := &Executor{tm: tm, se: se}

		res, err := exec.GetAllVertexesWithFieldValue("f", []byte("v"))
		assert.Nil(t, res)
		assert.ErrorContains(t, err, "failed to get vertices iterator")
		tm.AssertCalled(t, "RollbackTx", TxnID(1))
	})

	t.Run("iterator close fails", func(t *testing.T) {
		tm := new(mockTxnManager)
		se := new(MockStorageEngine)
		iter := new(mockAllVerticesIter)

		iter.seq = func(yield func(*Vertex) bool) {
			yield(&Vertex{ID: 1})
		}
		iter.close = errors.New("close fail")

		tm.On("Begin").Return(TxnID(1), nil)
		tm.On("RollbackTx", mock.Anything).Return(nil)
		tm.On("CommitTx", TxnID(1)).Return(nil)
		se.On("AllVerticesWithValue", TxnID(1), mock.Anything, mock.Anything).Return(iter, nil)

		exec := &Executor{tm: tm, se: se}

		res, err := exec.GetAllVertexesWithFieldValue("f", []byte("v"))
		assert.Len(t, res, 1)
		assert.ErrorContains(t, err, "close fail")
	})

	t.Run("commit fails", func(t *testing.T) {
		tm := new(mockTxnManager)
		se := new(MockStorageEngine)
		iter := new(mockAllVerticesIter)

		tm.On("Begin").Return(TxnID(2), nil)
		tm.On("CommitTx", TxnID(2)).Return(errors.New("commit fail"))
		tm.On("RollbackTx", TxnID(2)).Return(nil)
		se.On("AllVerticesWithValue", TxnID(2), "f", []byte("v")).Return(iter, nil)

		iter.seq = func(yield func(*Vertex) bool) {
			yield(&Vertex{ID: 2})
		}
		iter.close = nil

		exec := &Executor{tm: tm, se: se}
		res, err := exec.GetAllVertexesWithFieldValue("f", []byte("v"))
		assert.Len(t, res, 0)
		assert.ErrorContains(t, err, "failed to commit transaction")
	})

	t.Run("success", func(t *testing.T) {
		tm := new(mockTxnManager)
		se := new(MockStorageEngine)
		iter := new(mockAllVerticesIter)

		tm.On("Begin").Return(TxnID(3), nil)
		tm.On("CommitTx", TxnID(3)).Return(nil)
		se.On("AllVerticesWithValue", TxnID(3), "f", []byte("v")).Return(iter, nil)

		expected := []*Vertex{
			{ID: 1, Data: map[string]any{"f": "v"}},
			{ID: 2, Data: map[string]any{"f": "v"}},
		}

		iter.seq = func(yield func(*Vertex) bool) {
			for _, v := range expected {
				yield(v)
			}
		}

		exec := &Executor{tm: tm, se: se}
		res, err := exec.GetAllVertexesWithFieldValue("f", []byte("v"))
		assert.NoError(t, err)
		assert.Equal(t, expected, res)
	})
}

// Tests GetAllVertexesWithFieldValue2

func TestGetAllVertexesWithFieldValue2_StorageNil(t *testing.T) {
	exec := &Executor{se: nil}

	res, err := exec.GetAllVertexesWithFieldValue2("field", []byte("val"), nil, 1)
	assert.Nil(t, res)
	assert.ErrorContains(t, err, "storage engine is nil")
}

func TestGetAllVertexesWithFieldValue2_BeginFails(t *testing.T) {
	tm := new(mockTxnManager)
	tm.On("Begin").Return(TxnID(0), errors.New("begin failed"))

	exec := &Executor{tm: tm, se: new(MockStorageEngine)}
	res, err := exec.GetAllVertexesWithFieldValue2("f", []byte("v"), nil, 1)

	assert.Nil(t, res)
	assert.ErrorContains(t, err, "failed to begin transaction")
	tm.AssertExpectations(t)
}

func TestGetAllVertexesWithFieldValue2_AllVerticesFails(t *testing.T) {
	tm := new(mockTxnManager)
	tm.On("Begin").Return(TxnID(1), nil)
	tm.On("RollbackTx", TxnID(1)).Return(nil)

	se := new(MockStorageEngine)
	se.On("AllVerticesWithValue", TxnID(1), "f", []byte("v")).
		Return((*mockAllVerticesIter)(nil), errors.New("all vertices error"))

	exec := &Executor{tm: tm, se: se}
	res, err := exec.GetAllVertexesWithFieldValue2("f", []byte("v"), nil, 1)

	assert.Nil(t, res)
	assert.ErrorContains(t, err, "failed to get vertices iterator")
	tm.AssertExpectations(t)
	se.AssertExpectations(t)
}

func TestGetAllVertexesWithFieldValue2_CountEdgesFails(t *testing.T) {
	tm := new(mockTxnManager)
	tm.On("Begin").Return(TxnID(1), nil)
	tm.On("RollbackTx", TxnID(1)).Return(nil)

	iter := &mockAllVerticesIter{
		seq: func(yield func(*Vertex) bool) {
			yield(&Vertex{ID: 42})
		},
	}

	se := new(MockStorageEngine)
	se.On("AllVerticesWithValue", TxnID(1), "f", []byte("v")).
		Return(iter, nil)
	se.On("CountOfFilteredEdges", TxnID(1), VertexID(42), mock.Anything).
		Return(uint64(0), errors.New("count failed"))

	exec := &Executor{tm: tm, se: se}
	res, err := exec.GetAllVertexesWithFieldValue2("f", []byte("v"), nil, 1)

	assert.Nil(t, res)
	assert.ErrorContains(t, err, "failed to count edges")
	se.AssertExpectations(t)
}

func TestGetAllVertexesWithFieldValue2_SuccessPass(t *testing.T) {
	tm := new(mockTxnManager)
	tm.On("Begin").Return(TxnID(1), nil)
	tm.On("CommitTx", TxnID(1)).Return(nil)

	iter := &mockAllVerticesIter{
		seq: func(yield func(*Vertex) bool) {
			yield(&Vertex{ID: 1})
		},
	}

	se := new(MockStorageEngine)
	se.On("AllVerticesWithValue", TxnID(1), "f", []byte("v")).
		Return(iter, nil)
	se.On("CountOfFilteredEdges", TxnID(1), VertexID(1), mock.Anything).
		Return(uint64(5), nil)

	exec := &Executor{tm: tm, se: se}
	res, err := exec.GetAllVertexesWithFieldValue2("f", []byte("v"), nil, 3)

	assert.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, VertexID(1), res[0].ID)

	se.AssertExpectations(t)
	tm.AssertExpectations(t)
}

func TestGetAllVertexesWithFieldValue2_SuccessFiltered(t *testing.T) {
	tm := new(mockTxnManager)
	tm.On("Begin").Return(TxnID(1), nil)
	tm.On("CommitTx", TxnID(1)).Return(nil)

	iter := &mockAllVerticesIter{
		seq: func(yield func(*Vertex) bool) {
			yield(&Vertex{ID: 2})
		},
	}

	se := new(MockStorageEngine)
	se.On("AllVerticesWithValue", TxnID(1), "f", []byte("v")).
		Return(iter, nil)
	se.On("CountOfFilteredEdges", TxnID(1), VertexID(2), mock.Anything).
		Return(uint64(1), nil)

	exec := &Executor{tm: tm, se: se}
	res, err := exec.GetAllVertexesWithFieldValue2("f", []byte("v"), nil, 5)

	assert.NoError(t, err)
	assert.Len(t, res, 0)

	se.AssertExpectations(t)
	tm.AssertExpectations(t)
}

func TestGetAllVertexesWithFieldValue2_MultipleResults(t *testing.T) {
	tm := new(mockTxnManager)
	se := new(MockStorageEngine)
	iter := new(mockAllVerticesIter)

	v1 := &Vertex{ID: 1}
	v2 := &Vertex{ID: 2}
	v3 := &Vertex{ID: 3}
	v4 := &Vertex{ID: 4}

	iter.seq = func(yield func(*Vertex) bool) {
		yield(v1)
		yield(v2)
		yield(v3)
		yield(v4)
	}

	tm.On("Begin").Return(TxnID(1), nil)
	tm.On("CommitTx", TxnID(1)).Return(nil)
	se.On("AllVerticesWithValue", TxnID(1), "f", []byte("v")).Return(iter, nil)

	se.On("CountOfFilteredEdges", TxnID(1), VertexID(1), mock.Anything).Return(uint64(5), nil)
	se.On("CountOfFilteredEdges", TxnID(1), VertexID(2), mock.Anything).Return(uint64(2), nil)
	se.On("CountOfFilteredEdges", TxnID(1), VertexID(3), mock.Anything).Return(uint64(10), nil)
	se.On("CountOfFilteredEdges", TxnID(1), VertexID(4), mock.Anything).Return(uint64(3), nil)

	exec := &Executor{tm: tm, se: se}

	res, err := exec.GetAllVertexesWithFieldValue2("f", []byte("v"), func(e *Edge) bool { return true }, 3)
	require.NoError(t, err)
	require.Len(t, res, 3)
	assert.Contains(t, res, v1)
	assert.Contains(t, res, v3)
	assert.Contains(t, res, v4)
}

// SumNeighborAttributes

func TestSumAttributeOverProperNeighbors_SumCorrectly(t *testing.T) {
	se := new(MockStorageEngine)
	tm := new(mockTransactionManager)

	ex := &Executor{se: se, tm: tm}

	neighbors := []*Vertex{
		{ID: VertexID(1), Data: map[string]interface{}{"val": 2.0}},
		{ID: VertexID(2), Data: map[string]interface{}{"val": 3.5}},
	}

	iter := new(mockAllVerticesIter)
	iter.seq = func(yield func(*Vertex) bool) {
		yield(neighbors[0])
		yield(neighbors[1])
	}

	se.On("GetNeighborsWithEdgeFilter", mock.Anything, VertexID(1), mock.Anything).Return(iter, nil)

	res, err := ex.sumAttributeOverProperNeighbors(TxnID(1), &Vertex{ID: VertexID(1)}, "val", nil)
	assert.NoError(t, err)
	assert.Equal(t, 5.5, res)
}

func TestSumAttributeOverProperNeighbors_FieldMissing(t *testing.T) {
	se := new(MockStorageEngine)
	tm := new(mockTransactionManager)

	ex := &Executor{se: se, tm: tm}

	neighbors := []*Vertex{
		{ID: VertexID(1), Data: map[string]interface{}{}},
	}

	iter := new(mockAllVerticesIter)
	iter.seq = func(yield func(*Vertex) bool) {
		yield(neighbors[0])
	}

	se.On("GetNeighborsWithEdgeFilter", mock.Anything, VertexID(1), mock.Anything).Return(iter, nil)

	res, err := ex.sumAttributeOverProperNeighbors(TxnID(1), &Vertex{ID: VertexID(1)}, "val", nil)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, res)
}

func TestSumAttributeOverProperNeighbors_TypeMismatch(t *testing.T) {
	se := new(MockStorageEngine)

	ex := &Executor{se: se}

	neighbors := []*Vertex{
		{ID: VertexID(1), Data: map[string]interface{}{"val": "notfloat"}},
	}

	iter := new(mockAllVerticesIter)
	iter.seq = func(yield func(*Vertex) bool) {
		yield(neighbors[0])
	}

	se.On("GetNeighborsWithEdgeFilter", mock.Anything, VertexID(1), mock.Anything).Return(iter, nil)

	res, err := ex.sumAttributeOverProperNeighbors(TxnID(1), &Vertex{ID: VertexID(1)}, "val", nil)
	assert.Error(t, err)
	assert.Equal(t, 0.0, res)
}

func TestSumNeighborAttributes_SumAllVertices(t *testing.T) {
	se := new(MockStorageEngine)
	tm := new(mockTxnManager)

	ex := &Executor{se: se, tm: tm}

	tm.On("Begin").Return(TxnID(1), nil)
	tm.On("CommitTx", TxnID(1)).Return(nil)

	vertices := []*Vertex{
		{ID: VertexID(1)},
		{ID: VertexID(2)},
	}

	verticesIter := new(mockAllVerticesIter)
	verticesIter.seq = func(yield func(*Vertex) bool) {
		for _, v := range vertices {
			yield(v)
		}
	}

	se.On("GetAllVertices", TxnID(1)).Return(verticesIter, nil)

	neighborsV1 := []*Vertex{
		{ID: VertexID(11), Data: map[string]interface{}{"val": 1.0}},
	}
	neighborsV2 := []*Vertex{
		{ID: VertexID(12), Data: map[string]interface{}{"val": 1.0}},
	}

	iterV1 := new(mockAllVerticesIter)
	iterV1.seq = func(yield func(*Vertex) bool) {
		for _, v := range neighborsV1 {
			yield(v)
		}
	}

	iterV2 := new(mockAllVerticesIter)
	iterV2.seq = func(yield func(*Vertex) bool) {
		for _, v := range neighborsV2 {
			yield(v)
		}
	}

	se.On("GetNeighborsWithEdgeFilter", TxnID(1), VertexID(1), mock.Anything).Return(iterV1, nil)
	se.On("GetNeighborsWithEdgeFilter", TxnID(1), VertexID(2), mock.Anything).Return(iterV2, nil)
	se.On("NewAggregationAssociativeArray", TxnID(1)).Return(NewInMemoryAssociativeArray[VertexID, float64](), nil)

	resAA, err := ex.SumNeighborAttributes("val", nil, func(f float64) bool {
		return true
	})
	assert.NoError(t, err)

	v1val, ok1 := resAA.Get(VertexID(1))
	v2val, ok2 := resAA.Get(VertexID(2))

	assert.True(t, ok1)
	assert.True(t, ok2)
	assert.Equal(t, 1.0, v1val)
	assert.Equal(t, 1.0, v2val)

	tm.AssertExpectations(t)
	se.AssertExpectations(t)
}

// GetAllTriangles

func TestGetAllTriangles_EmptyGraph(t *testing.T) {
	se := newDataMockStorageEngine(nil, nil, nil, nil, nil, nil)
	tm := &mockTransactionManager{nextTxnID: 1}
	e := &Executor{se: se, tm: tm}

	count, err := e.GetAllTriangles()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), count)
}

func TestGetAllTriangles_K3(t *testing.T) {
	vertices := []VertexID{1, 2, 3}
	edges := [][]VertexID{{1, 2}, {2, 3}, {1, 3}}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)
	tm := &mockTransactionManager{nextTxnID: 1}
	e := &Executor{se: se, tm: tm}

	count, err := e.GetAllTriangles()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count, "K3 должен содержать ровно 1 треугольник")
}

func TestGetAllTriangles_K4(t *testing.T) {
	vertices := []VertexID{1, 2, 3, 4}
	edges := [][]VertexID{{1, 2}, {1, 3}, {1, 4}, {2, 3}, {2, 4}, {3, 4}}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)
	tm := &mockTransactionManager{nextTxnID: 1}
	e := &Executor{se: se, tm: tm}

	count, err := e.GetAllTriangles()
	require.NoError(t, err)
	assert.Equal(t, uint64(4), count, "K4 должен содержать ровно 4 треугольника")
}

func TestGetAllTriangles_DisconnectedWithTriangle(t *testing.T) {
	vertices := []VertexID{1, 2, 3, 4}
	edges := [][]VertexID{{1, 2}, {2, 3}, {1, 3}}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)
	tm := &mockTransactionManager{nextTxnID: 1}
	e := &Executor{se: se, tm: tm}

	count, err := e.GetAllTriangles()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count)
}

func TestGetAllTriangles_SelfLoop(t *testing.T) {
	vertices := []VertexID{1, 2, 3}
	edges := [][]VertexID{{1, 1}, {1, 2}, {2, 3}, {1, 3}}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)
	tm := &mockTransactionManager{nextTxnID: 1}
	e := &Executor{se: se, tm: tm}

	count, err := e.GetAllTriangles()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count)
}

func TestGetAllTriangles_TransactionBeginError(t *testing.T) {
	se := newDataMockStorageEngine(nil, nil, nil, nil, nil, nil)
	tm := &mockTransactionManager{beginErr: errors.New("failed to begin tx")}
	e := &Executor{se: se, tm: tm}

	count, err := e.GetAllTriangles()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to begin tx")
	assert.Equal(t, uint64(0), count)
}

func TestGetAllTriangles_CommitError(t *testing.T) {
	vertices := []VertexID{1, 2, 3}
	edges := [][]VertexID{{1, 2}, {2, 3}, {1, 3}}
	se := newDataMockStorageEngine(vertices, edges, nil, nil, nil, nil)
	tm := &mockTransactionManager{nextTxnID: 1, commitErr: errors.New("commit failed")}
	e := &Executor{se: se, tm: tm}

	count, err := e.GetAllTriangles()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit failed")
	assert.Equal(t, uint64(0), count)
}

func TestGetAllTriangles_NeighborsError(t *testing.T) {
	vertices := []VertexID{1, 2, 3}
	edges := [][]VertexID{{1, 2}, {2, 3}, {1, 3}}
	se := newDataMockStorageEngine(vertices, edges, errors.New("failed to get neighbors"), nil, nil, nil)
	tm := &mockTransactionManager{nextTxnID: 1}
	e := &Executor{se: se, tm: tm}

	count, err := e.GetAllTriangles()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get neighbors")
	assert.Equal(t, uint64(0), count)
}
