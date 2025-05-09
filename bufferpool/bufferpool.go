package bufferpool

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Blackdeer1524/GraphDB/pkg/assert"
)

const noFrame = ^uint64(0)

var ErrNoSuchPage = errors.New("no such page")

type Page interface {
	GetData() []byte

	// latch methods
	Lock()
	Unlock()
	RLock()
	RUnlock()
}

type Replacer interface {
	Pin(frameID uint64)
	Unpin(frameID uint64)
	ChooseVictim() (uint64, error)
	GetSize() uint64
}

type DiskManager[T Page] interface {
	ReadPage(fileID, pageID uint64) (T, error)
	WritePage(page T) error
}

type Frame[T Page] struct {
	Page     T
	Idx      uint64
	PinCount int
	Dirty    bool
	FileID   uint64
	PageID   uint64
}

type pageIdentity struct {
	fileID uint64
	pageID uint64
}

type Manager[T Page] struct {
	poolSize    int
	pageToFrame map[pageIdentity]uint64
	frames      []Frame[T]
	emptyFrames []uint64

	replacer Replacer

	diskManager DiskManager[T]

	fastPath sync.Mutex
	slowPath sync.Mutex
}

func New[T Page](poolSize int, replacer Replacer, diskManager DiskManager[T]) (*Manager[T], error) {
	assert.Assert(poolSize > 0, "pool size must be greater than zero")

	emptyFrames := make([]uint64, poolSize)
	for i := 0; i < poolSize; i++ {
		emptyFrames[i] = uint64(i)
	}

	frames := make([]Frame[T], poolSize)

	return &Manager[T]{
		poolSize:    poolSize,
		pageToFrame: make(map[pageIdentity]uint64),
		frames:      frames,
		emptyFrames: emptyFrames,
		replacer:    replacer,
		diskManager: diskManager,
	}, nil
}

func (m *Manager[T]) Unpin(fileID, pageID uint64) error {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	frameID, ok := m.pageToFrame[pageIdentity{fileID, pageID}]
	if !ok {
		return ErrNoSuchPage
	}

	m.unpinFrame(frameID)

	return nil
}

func (m *Manager[T]) unpinFrame(frameID uint64) {
	frame := &m.frames[frameID]

	assert.Assert(frame.PinCount > 0, "invalid pin count")

	frame.PinCount--
	if frame.PinCount == 0 {
		m.replacer.Unpin(frameID)
	}
}

func (m *Manager[T]) pin(fileID, pageID uint64) {
	frameID, ok := m.pageToFrame[pageIdentity{fileID, pageID}]

	assert.Assert(ok, "no frame for page: %d", pageID)

	m.frames[frameID].PinCount++
	m.replacer.Pin(frameID)
}

func (m *Manager[T]) GetPage(fileID, pageID uint64) (*T, error) {
	m.fastPath.Lock()

	pIdent := pageIdentity{fileID, pageID}

	if frameID, ok := m.pageToFrame[pIdent]; ok {
		m.pin(fileID, pageID)
		m.fastPath.Unlock()

		return &m.frames[frameID].Page, nil
	}

	m.fastPath.Unlock()

	m.slowPath.Lock()
	defer m.slowPath.Unlock()

	m.fastPath.Lock()
	if frameID, ok := m.pageToFrame[pIdent]; ok {
		m.pin(fileID, pageID)
		m.fastPath.Unlock()

		return &m.frames[frameID].Page, nil
	}
	m.fastPath.Unlock()

	frameID := m.reserveFrame()
	if frameID != noFrame {
		page, err := m.diskManager.ReadPage(fileID, pageID)
		if err != nil {
			return nil, err
		}
		m.frames[frameID] = Frame[T]{
			Page:     page,
			PinCount: 1,
			FileID:   fileID,
			PageID:   pageID,
		}
		m.pageToFrame[pIdent] = frameID
		return &page, nil
	}

	victimFrameID, err := m.replacer.ChooseVictim()
	if err != nil {
		return nil, err
	}

	victimFrame := &m.frames[victimFrameID]
	if victimFrame.Dirty {
		err = m.diskManager.WritePage(victimFrame.Page)
		if err != nil {
			return nil, err
		}
	}

	oldIdent := pageIdentity{
		fileID: victimFrame.FileID,
		pageID: victimFrame.PageID,
	}

	delete(m.pageToFrame, oldIdent)

	page, err := m.diskManager.ReadPage(fileID, pageID)
	if err != nil {
		return nil, err
	}

	m.frames[victimFrameID] = Frame[T]{
		Page:     page,
		PinCount: 1,
		FileID:   fileID,
		PageID:   pageID,
	}

	m.pageToFrame[pIdent] = victimFrameID

	return &page, nil
}

func (m *Manager[T]) reserveFrame() uint64 {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	if len(m.emptyFrames) > 0 {
		id := m.emptyFrames[0]
		m.emptyFrames = m.emptyFrames[1:]
		m.frames[id].PinCount = 1
		m.replacer.Pin(id)

		return id
	}

	return noFrame
}

func (m *Manager[T]) MarkDirty(fileID, pageID uint64) error {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	frameID, ok := m.pageToFrame[pageIdentity{fileID, pageID}]
	if !ok {
		return ErrNoSuchPage
	}

	m.frames[frameID].Dirty = true
	return nil
}

func (m *Manager[T]) FlushPage(fileID, pageID uint64) error {
	pIdent := pageIdentity{fileID, pageID}
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	frameID, ok := m.pageToFrame[pIdent]
	if !ok {
		return fmt.Errorf("no frame for such page: %d", pageID)
	}

	frame := &m.frames[frameID]
	if !frame.Dirty {
		return nil
	}

	err := m.diskManager.WritePage(frame.Page)
	if err != nil {
		return fmt.Errorf("failed to write page to disk: %w", err)

	}

	frame.Dirty = false

	return nil
}

func (m *Manager[T]) FlushAllPages() error {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	for i := range m.frames {
		frame := &m.frames[i]
		if frame.Dirty {
			_ = m.diskManager.WritePage(frame.Page)

			frame.Dirty = false
		}
	}

	return nil
}
