package disk

import (
	"github.com/Blackdeer1524/GraphDB/storage/page"
)

type Manager struct {
	fileIDToPath map[uint64]string
}

type Page interface {
	GetData() []byte
	Insert(record []byte) (int, error)
}

func NewManager(fileIDToPath map[uint64]string) *Manager {
	return &Manager{
		fileIDToPath: fileIDToPath,
	}
}

func (m *Manager) ReadPage() (*page.SlottedPage, error) {
	return nil, nil
}
