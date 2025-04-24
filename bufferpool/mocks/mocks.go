package mocks

import (
	"errors"
	"github.com/Blackdeer1524/GraphDB/bufferpool"
	"github.com/stretchr/testify/mock"
)

type MockPage struct {
	Data []byte
}

func (p *MockPage) GetData() []byte {
	return p.Data
}

func (p *MockPage) Insert(record []byte) (int, error) {
	p.Data = append(p.Data, record...)

	return len(p.Data), nil
}

type mockDiskManager struct {
	mock.Mock
	Pages map[[2]uint64]*MockPage
}

func NewMockDiskManager() *mockDiskManager {
	return &mockDiskManager{
		Pages: make(map[[2]uint64]*MockPage),
	}
}

func (d *mockDiskManager) ReadPage(fileID, pageID uint64) (bufferpool.Page, error) {
	key := [2]uint64{fileID, pageID}
	if page, ok := d.Pages[key]; ok {
		return page, nil
	}

	return nil, errors.New("not found")
}

func (d *mockDiskManager) WritePage(p bufferpool.Page) error {
	return nil
}

type mockReplacer struct {
	Pinned  map[uint64]bool
	victims []uint64
}

func NewMockReplacer() *mockReplacer {
	return &mockReplacer{
		Pinned:  make(map[uint64]bool),
		victims: make([]uint64, 0),
	}
}

func (r *mockReplacer) Pin(frameID uint64) {
	r.Pinned[frameID] = true
}

func (r *mockReplacer) Unpin(frameID uint64) {
	r.Pinned[frameID] = false
	r.victims = append(r.victims, frameID)
}

func (r *mockReplacer) ChooseVictim() (uint64, error) {
	if len(r.victims) == 0 {
		return 0, errors.New("no victim")
	}

	v := r.victims[0]
	r.victims = r.victims[1:]

	return v, nil
}

func (r *mockReplacer) GetSize() uint64 {
	return uint64(len(r.victims))
}
