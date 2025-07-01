package bufferpool

import (
	"github.com/stretchr/testify/mock"
)

type SlottedPage_mock struct {
	data  []byte
	dirty bool
}

func (p *SlottedPage_mock) GetData() []byte       { return p.data }
func (p *SlottedPage_mock) SetData(d []byte)      { p.data = d }
func (p *SlottedPage_mock) SetDirtiness(val bool) { p.dirty = val }
func (p *SlottedPage_mock) IsDirty() bool         { return p.dirty }
func (p *SlottedPage_mock) Lock()                 {}
func (p *SlottedPage_mock) Unlock()               {}
func (p *SlottedPage_mock) RLock()                {}
func (p *SlottedPage_mock) RUnlock()              {}

// Фабрика для создания SlottedPage
func NewSlottedPage_mock() *SlottedPage_mock {
	return &SlottedPage_mock{
		data:  []byte{},
		dirty: false,
	}
}

// Мок DiskManager
type MockDiskManager struct {
	mock.Mock
}

func (m *MockDiskManager) ReadPage(
	pageIdent PageIdentity,
) (*SlottedPage_mock, error) {
	args := m.Called(pageIdent)
	return args.Get(0).(*SlottedPage_mock), args.Error(1)
}

func (m *MockDiskManager) WritePage(
	page *SlottedPage_mock,
	pageIdent PageIdentity,
) error {
	args := m.Called(page, pageIdent)
	return args.Error(0)
}

// Мок Replacer
type MockReplacer struct {
	mock.Mock
}

func (m *MockReplacer) Pin(frameID uint64) {
	m.Called(frameID)
}

func (m *MockReplacer) Unpin(frameID uint64) {
	m.Called(frameID)
}

func (m *MockReplacer) ChooseVictim() (uint64, error) {
	args := m.Called()
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockReplacer) GetSize() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}
