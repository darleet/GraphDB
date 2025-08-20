package inmemory

type InMemoryAssociativeArray[K comparable, V any] struct {
	data map[K]V
}

func NewInMemoryAssociativeArray[K comparable, V any]() *InMemoryAssociativeArray[K, V] {
	return &InMemoryAssociativeArray[K, V]{
		data: make(map[K]V),
	}
}

func (a *InMemoryAssociativeArray[K, V]) Get(k K) (V, bool) {
	v, ok := a.data[k]

	return v, ok
}

func (a *InMemoryAssociativeArray[K, V]) Set(k K, v V) error {
	a.data[k] = v

	return nil
}

func (a *InMemoryAssociativeArray[K, V]) Seq(yield func(K, V) bool) {
	for k, v := range a.data {
		if !yield(k, v) {
			break
		}
	}
}
