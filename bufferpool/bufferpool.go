package bufferpool

import (
	"errors"
	"fmt"
	"sync"
)

const noFrame = ^uint64(0)

var ErrNoSuchPage = errors.New("no such page")

// Replacer manages what page is next for eviction from RAM to disk.
type Replacer interface {
	Pin(frameID uint64)
	Unpin(frameID uint64)
	ChooseVictim() (uint64, error)
	GetSize() uint64
}

type Page interface {
	GetData() []byte
	Insert(record []byte) (int, error)
}

type DiskManager interface {
	ReadPage(fileID uint64, pageID uint64) (Page, error)
	WritePage(page Page) error
}

// Frame is a "container" for one page in buffer pool with some metadata.
// Frame is stored in physical memory (RAM)
type Frame struct {
	page Page

	idx uint64

	pinCount int

	dirty bool

	latch sync.RWMutex

	fileID uint64
	pageID uint64
}

type pageIdentity struct {
	fileID uint64
	pageID uint64
}

type Manager struct {
	poolSize int

	pageToFrame map[pageIdentity]uint64
	frames      []*Frame
	emptyFrames []uint64

	replacer Replacer

	diskManager DiskManager

	fastPath sync.Mutex
	slowPath sync.Mutex
}

// New creates buffer pool (or page cache) object with specified poolSize.
// poolSize is a count of pages, that can be stored in physical memory (RAM)
// at the same time
func New(poolSize int, replacer Replacer, diskManager DiskManager) (*Manager, error) {
	if poolSize <= 0 {
		return nil, errors.New("pool size must be greater than zero")
	}

	emptyFrames := make([]uint64, poolSize)
	for i := 0; i < poolSize; i++ {
		emptyFrames[i] = uint64(i)
	}

	return &Manager{
		poolSize:    poolSize,
		pageToFrame: make(map[pageIdentity]uint64),
		frames:      make([]*Frame, poolSize),
		emptyFrames: emptyFrames,
		replacer:    replacer,
		diskManager: diskManager,
	}, nil
}

// Unpin decreases the pin count of the page identified by fileID and pageID.
// If the pin count drops to zero, the page becomes a candidate for eviction
// and is passed to the replacer.
func (m *Manager) Unpin(fileID, pageID uint64) error {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	frameID, ok := m.pageToFrame[pageIdentity{fileID, pageID}]
	if !ok {
		return ErrNoSuchPage
	}

	return m.unpinFrame(frameID)
}

func (m *Manager) unpinFrame(frameID uint64) error {
	frame := m.frames[frameID]
	if frame.pinCount <= 0 {
		return errors.New("invalid pin count")
	}

	frame.pinCount--
	if frame.pinCount == 0 {
		m.replacer.Unpin(frameID)
	}

	return nil
}

func (m *Manager) pin(fileID, pageID uint64) error {
	frameID, ok := m.pageToFrame[pageIdentity{fileID, pageID}]
	if !ok {
		return fmt.Errorf("no frame for such page: %d", pageID)
	}

	frame := m.frames[frameID]
	frame.pinCount++
	m.replacer.Pin(frameID)

	return nil
}

// GetPage returns the page identified by fileID and pageID from the buffer pool.
// If the page is already cached, it increments the pin count and returns it.
// Otherwise, it tries to:
//   - allocate a free frame if available, or
//   - evict a victim frame using the replacer (writing it back to disk if dirty),
//
// then loads the requested page from disk into that frame.
func (m *Manager) GetPage(fileID, pageID uint64) (Page, error) {
	m.fastPath.Lock()

	pIdent := pageIdentity{fileID, pageID}

	if frameID, ok := m.pageToFrame[pIdent]; ok {
		err := m.pin(fileID, pageID)
		m.fastPath.Unlock()
		if err != nil {
			return nil, err
		}
		return m.frames[frameID].page, nil
	}

	m.fastPath.Unlock()

	m.slowPath.Lock()
	defer m.slowPath.Unlock()

	m.fastPath.Lock()

	if frameID, ok := m.pageToFrame[pIdent]; ok {
		err := m.pin(fileID, pageID)
		m.fastPath.Unlock()
		if err != nil {
			return nil, err
		}

		return m.frames[frameID].page, nil
	}

	m.fastPath.Unlock()

	frameID := m.reserveFrame()
	if frameID != noFrame {
		page, err := m.diskManager.ReadPage(fileID, pageID)
		if err != nil {
			return nil, err
		}

		m.frames[frameID] = &Frame{
			page:     page,
			pinCount: 1,
			fileID:   fileID,
			pageID:   pageID,
		}

		m.pageToFrame[pIdent] = frameID
		return page, nil
	}

	victimFrameID, err := m.replacer.ChooseVictim()
	if err != nil {
		return nil, err
	}

	victimFrame := m.frames[victimFrameID]
	if victimFrame == nil {
		return nil, errors.New("victim frame is nil")
	}

	if victimFrame.dirty {
		err = m.diskManager.WritePage(victimFrame.page)
		if err != nil {
			return nil, err
		}
	}

	// Remove old page mapping
	oldIdent := pageIdentity{fileID: victimFrame.fileID, pageID: victimFrame.pageID}
	delete(m.pageToFrame, oldIdent)

	// Load new page
	page, err := m.diskManager.ReadPage(fileID, pageID)
	if err != nil {
		return nil, err
	}

	m.frames[victimFrameID] = &Frame{
		page:     page,
		pinCount: 1,
		fileID:   fileID,
		pageID:   pageID,
	}

	m.pageToFrame[pIdent] = victimFrameID

	return page, nil
}

func (m *Manager) reserveFrame() uint64 {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	if len(m.emptyFrames) > 0 {
		id := m.emptyFrames[0]
		m.emptyFrames = m.emptyFrames[1:]

		if m.frames[id] == nil {
			m.frames[id] = &Frame{}
		}

		m.frames[id].pinCount++
		m.replacer.Pin(id)
		return id
	}

	return noFrame
}

func (m *Manager) MarkDirty(fileID, pageID uint64) error {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	frameID, ok := m.pageToFrame[pageIdentity{fileID, pageID}]
	if !ok {
		return ErrNoSuchPage
	}

	m.frames[frameID].dirty = true

	return nil
}

// FlushPage writes the page to disk if and only if it is marked as dirty.
func (m *Manager) FlushPage(fileID, pageID uint64) error {
	pIdent := pageIdentity{fileID, pageID}

	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	frameID, ok := m.pageToFrame[pIdent]
	if !ok {
		return fmt.Errorf("no frame for such page: %d", pageID)
	}

	frame := m.frames[frameID]
	if !frame.dirty {
		return nil
	}

	err := m.diskManager.WritePage(frame.page)
	if err != nil {
		return fmt.Errorf("failed to write page to disk: %w", err)
	}

	frame.dirty = false

	return nil
}

// FlushAllPages flushes all dirty pages to disk
func (m *Manager) FlushAllPages() error {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	for _, frame := range m.frames {
		if frame != nil && frame.dirty {
			err := m.diskManager.WritePage(frame.page)
			if err != nil {
				return err
			}

			frame.dirty = false
		}
	}

	return nil
}
