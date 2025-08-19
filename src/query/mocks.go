package query

import (
	"fmt"
	"iter"

	"github.com/stretchr/testify/mock"
)

// Mock implementations

// mockQueue is a simple in-memory queue for testing.
type mockQueue struct {
	items []VertexWithDepthAndRID
}

func (m *mockQueue) Enqueue(v VertexWithDepthAndRID) error {
	m.items = append(m.items, v)
	return nil
}

func (m *mockQueue) Dequeue() (VertexWithDepthAndRID, error) {
	if len(m.items) == 0 {
		return VertexWithDepthAndRID{}, ErrQueueEmpty
	}
	v := m.items[0]
	m.items = m.items[1:]
	return v, nil
}

func (m *mockQueue) Close() error {
	return nil
}

// mockVisited is a simple map-based visited set.
type mockVisited struct {
	visited map[VertexID]bool
}

func newMockVisited() *mockVisited {
	return &mockVisited{visited: make(map[VertexID]bool)}
}

func (m *mockVisited) Get(v VertexID) (bool, error) {
	val, ok := m.visited[v]
	return ok && val, nil
}

func (m *mockVisited) Set(v VertexID, val bool) error {
	m.visited[v] = val
	return nil
}

func (m *mockVisited) Close() error {
	return nil
}

// mockIterator for neighbors.
type mockIterator struct {
	vertices []VertexIDWithRID
	index    int
}

func newMockIterator(vertices []VertexIDWithRID) *mockIterator {
	return &mockIterator{vertices: vertices, index: -1}
}

func (m *mockIterator) Next() (bool, error) {
	m.index++
	return m.index < len(m.vertices), nil
}

func (m *mockIterator) Vertex() VertexIDWithRID {
	if m.index >= 0 && m.index < len(m.vertices) {
		return m.vertices[m.index]
	}
	return VertexIDWithRID{}
}

func (m *mockIterator) Close() error {
	return nil
}

type mockAllVerticesIter struct {
	seq   func(yield func(*Vertex) bool)
	close error
}

func (m *mockAllVerticesIter) Seq() iter.Seq[*Vertex] {
	return m.seq
}

func (m *mockAllVerticesIter) Close() error {
	return m.close
}

type mockNeighborsIterator struct {
	items []*VertexIDWithRID
	idx   int
}

func newNeighborsMockIterator(items []VertexIDWithRID) NeighborIter {
	ptrs := make([]*VertexIDWithRID, len(items))
	for i := range items {
		ptrs[i] = &items[i]
	}

	return &mockNeighborsIterator{items: ptrs}
}

func (m *mockNeighborsIterator) Seq() iter.Seq[*VertexIDWithRID] {
	return func(yield func(*VertexIDWithRID) bool) {
		for _, it := range m.items {
			if !yield(it) {
				return
			}
		}
	}
}

func (m *mockNeighborsIterator) Close() error {
	return nil
}

// mockTransactionManager
type mockTransactionManager struct {
	beginErr    error
	commitErr   error
	rollbackErr error
	nextTxnID   TxnID
}

func (m *mockTransactionManager) Begin() (TxnID, error) {
	if m.beginErr != nil {
		return 0, m.beginErr
	}
	m.nextTxnID++
	return m.nextTxnID, nil
}

func (m *mockTransactionManager) CommitTx(_ TxnID) error {
	return m.commitErr
}

func (m *mockTransactionManager) RollbackTx(_ TxnID) error {
	return m.rollbackErr
}

type mockTxnManager struct {
	mock.Mock
}

func (m *mockTxnManager) Begin() (TxnID, error) {
	args := m.Called()

	return args.Get(0).(TxnID), args.Error(1)
}
func (m *mockTxnManager) CommitTx(tx TxnID) error {
	args := m.Called(tx)

	return args.Error(0)
}
func (m *mockTxnManager) RollbackTx(tx TxnID) error {
	args := m.Called(tx)
	return args.Error(0)
}

type DataMockStorageEngine struct {
	vertices     map[VertexID]RID
	neighbors    map[VertexID][]VertexIDWithRID
	queueErr     error
	bitMapErr    error
	getRIDErr    error
	neighborsErr error
}

// newDataMockStorageEngine создаёт DataMockStorageEngine с заданным графом.
func newDataMockStorageEngine(vertices []VertexID, edges [][]VertexID, neighborsErr, queueErr, bitMapErr, getRIDErr error) *DataMockStorageEngine {
	se := &DataMockStorageEngine{
		vertices:     make(map[VertexID]RID),
		neighbors:    make(map[VertexID][]VertexIDWithRID),
		neighborsErr: neighborsErr,
		queueErr:     queueErr,
		bitMapErr:    bitMapErr,
		getRIDErr:    getRIDErr,
	}
	for _, v := range vertices {
		se.vertices[v] = RID{PageID: uint64(v * 100)}
	}
	for _, edge := range edges {
		if len(edge) != 2 {
			panic("edge must be a pair [u, v]")
		}
		u, v := edge[0], edge[1]
		se.neighbors[u] = append(se.neighbors[u], VertexIDWithRID{V: v, R: RID{PageID: uint64(v * 100)}})
		se.neighbors[v] = append(se.neighbors[v], VertexIDWithRID{V: u, R: RID{PageID: uint64(u * 100)}})
	}
	return se
}

func (m *DataMockStorageEngine) NewAggregationAssociativeArray(TxnID) (AssociativeArray[VertexID, float64], error) {
	return NewInMemoryAssociativeArray[VertexID, float64](), nil
}

func (m *DataMockStorageEngine) GetNeighborsWithEdgeFilter(t TxnID, v VertexID, filter EdgeFilter) (VerticesIter, error) {
	return &mockAllVerticesIter{}, nil
}

func (m *DataMockStorageEngine) GetAllVertices(t TxnID) (VerticesIter, error) {
	vertices := make([]*Vertex, 0, len(m.vertices))
	for v := range m.vertices {
		vertices = append(vertices, &Vertex{ID: v})
	}

	n := &mockAllVerticesIter{}

	n.seq = func(yield func(*Vertex) bool) {
		for _, v := range vertices {
			if !yield(v) {
				return
			}
		}
	}

	return n, nil
}

func (m *DataMockStorageEngine) CountOfFilteredEdges(t TxnID, v VertexID, f EdgeFilter) (uint64, error) {
	return 0, nil
}

func (m *DataMockStorageEngine) AllVerticesWithValue(t TxnID, field string, value []byte) (VerticesIter, error) {
	return &mockAllVerticesIter{}, nil
}

func (m *DataMockStorageEngine) NewQueue(_ TxnID) (Queue, error) {
	if m.queueErr != nil {
		return nil, m.queueErr
	}
	return &mockQueue{}, nil
}

func (m *DataMockStorageEngine) NewBitMap(_ TxnID) (Visited, error) {
	if m.bitMapErr != nil {
		return nil, m.bitMapErr
	}
	return newMockVisited(), nil
}

func (m *DataMockStorageEngine) GetVertexRID(_ TxnID, v VertexID) (VertexIDWithRID, error) {
	if m.getRIDErr != nil {
		return VertexIDWithRID{}, m.getRIDErr
	}
	rid, ok := m.vertices[v]
	if !ok {
		return VertexIDWithRID{}, fmt.Errorf("vertex not found")
	}
	return VertexIDWithRID{V: v, R: rid}, nil
}

func (m *DataMockStorageEngine) Neighbors(_ TxnID, v VertexID) (NeighborIter, error) {
	if m.neighborsErr != nil {
		return nil, m.neighborsErr
	}
	neighs, ok := m.neighbors[v]
	if !ok {
		return newNeighborsMockIterator(nil), nil
	}
	return newNeighborsMockIterator(neighs), nil
}

// MockStorageEngine использует mock.Mock для всех методов.
type MockStorageEngine struct {
	mock.Mock
}

func (m *MockStorageEngine) NewAggregationAssociativeArray(t TxnID) (AssociativeArray[VertexID, float64], error) {
	args := m.Called(t)
	return args.Get(0).(AssociativeArray[VertexID, float64]), args.Error(1)
}

func (m *MockStorageEngine) GetNeighborsWithEdgeFilter(t TxnID, v VertexID, filter EdgeFilter) (VerticesIter, error) {
	args := m.Called(t, v, filter)
	return args.Get(0).(VerticesIter), args.Error(1)
}

func (m *MockStorageEngine) GetAllVertices(t TxnID) (VerticesIter, error) {
	args := m.Called(t)
	return args.Get(0).(VerticesIter), args.Error(1)
}

func (m *MockStorageEngine) CountOfFilteredEdges(t TxnID, v VertexID, f EdgeFilter) (uint64, error) {
	args := m.Called(t, v, f)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockStorageEngine) AllVerticesWithValue(t TxnID, field string, value []byte) (VerticesIter, error) {
	args := m.Called(t, field, value)
	return args.Get(0).(VerticesIter), args.Error(1)
}

func (m *MockStorageEngine) NewQueue(t TxnID) (Queue, error) {
	args := m.Called(t)
	return args.Get(0).(Queue), args.Error(1)
}

func (m *MockStorageEngine) NewBitMap(t TxnID) (Visited, error) {
	args := m.Called(t)
	return args.Get(0).(Visited), args.Error(1)
}

func (m *MockStorageEngine) GetVertexRID(t TxnID, v VertexID) (VertexIDWithRID, error) {
	args := m.Called(t, v)
	return args.Get(0).(VertexIDWithRID), args.Error(1)
}

func (m *MockStorageEngine) Neighbors(t TxnID, v VertexID) (NeighborIter, error) {
	args := m.Called(t, v)
	return args.Get(0).(NeighborIter), args.Error(1)
}
