package bufferpool

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

const noFrame = ^uint64(0)

var ErrNoSuchPage = errors.New("no such page")

type Page interface {
	GetData() []byte
	SetData(d []byte)

	SetDirtiness(val bool)
	IsDirty() bool

	// latch methods
	Lock()
	Unlock()
	RLock()
	RUnlock()
}

var (
	_ Page = &page.SlottedPage{}
)

type Replacer interface {
	Pin(frameID uint64)
	Unpin(frameID uint64)
	ChooseVictim() (uint64, error)
	GetSize() uint64
}

type DiskManager[T Page] interface {
	ReadPage(pageIdent common.PageIdentity) (T, error)
	WritePage(page T, pageIdent common.PageIdentity) error
}

type BufferPool[T Page] interface {
	Unpin(common.PageIdentity) error
	GetPage(common.PageIdentity) (T, error)
	GetPageNoCreate(common.PageIdentity) (T, error)
	FlushPage(common.PageIdentity) error
}

type frameInfo struct {
	frameID  uint64
	pinCount uint64
	isDirty  bool
}

type Manager struct {
	poolSize        uint64
	pageToFrameInfo map[common.PageIdentity]frameInfo
	frames          []page.SlottedPage
	emptyFrames     []uint64

	replacer Replacer

	diskManager    DiskManager[*page.SlottedPage]
	DirtyPageTable map[common.PageIdentity]common.LSN

	fastPath sync.Mutex
	slowPath sync.Mutex
}

var (
	_ BufferPool[Page] = &Manager{}
)

func (m *Manager) Unpin(pIdent common.PageIdentity) error {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	frameInfo, ok := m.pageToFrameInfo[pIdent]
	if !ok {
		return ErrNoSuchPage
	}

	assert.Assert(frameInfo.pinCount > 0, "invalid pin count")

	frameInfo.pinCount--
	m.pageToFrameInfo[pIdent] = frameInfo
	if frameInfo.pinCount == 0 {
		m.replacer.Unpin(frameInfo.frameID)
	}

	return nil
}

func (m *Manager) pin(pIdent common.PageIdentity) {
	// WARN: m has to locked!
	frameInfo, ok := m.pageToFrameInfo[pIdent]

	assert.Assert(ok, "no frame for page: %v", pIdent)

	frameInfo.pinCount++
	m.pageToFrameInfo[pIdent] = frameInfo
	m.replacer.Pin(frameInfo.frameID)
}

func (m *Manager) GetPageNoCreate(pageID common.PageIdentity) (*page.SlottedPage, error) {
	panic("NOT IMPLEMENTED")
}

func (m *Manager) GetPage(pIdent common.PageIdentity) (*page.SlottedPage, error) {
	m.fastPath.Lock()

	if frameInfo, ok := m.pageToFrameInfo[pIdent]; ok {
		m.pin(pIdent)
		m.fastPath.Unlock()

		return &m.frames[frameInfo.frameID], nil
	}

	m.fastPath.Unlock()

	m.slowPath.Lock()
	defer m.slowPath.Unlock()

	m.fastPath.Lock()
	if frameInfo, ok := m.pageToFrameInfo[pIdent]; ok {
		m.pin(pIdent)
		m.fastPath.Unlock()

		return &m.frames[frameInfo.frameID], nil
	}
	m.fastPath.Unlock()

	frameID := m.reserveFrame()
	if frameID != noFrame {
		page, err := m.diskManager.ReadPage(common.PageIdentity{
			FileID: pIdent.FileID,
			PageID: pIdent.PageID,
		})
		if err != nil {
			return nil, err
		}

		m.frames[frameID] = frame[T]{
			Page:     page,
			PinCount: 1,
			PageIdent: common.PageIdentity{
				FileID: pIdent.FileID,
				PageID: pIdent.PageID,
			},
		}
		m.pageToFrameInfo[pIdent] = frameID

		return page, nil
	}

	victimFrameID, err := m.replacer.ChooseVictim()
	if err != nil {
		var zero T
		return zero, err
	}

	victimFrame := m.frames[victimFrameID]
	if victimFrame.Page.IsDirty() {
		err = m.diskManager.WritePage(
			victimFrame.Page,
			victimFrame.PageIdent,
		)
		if err != nil {
			var zero T
			return zero, err
		}

		delete(m.DirtyPageTable, pIdent)
	}

	delete(m.pageToFrameInfo, victimFrame.PageIdent)

	page, err := m.diskManager.ReadPage(common.PageIdentity{
		FileID: pIdent.FileID,
		PageID: pIdent.PageID,
	})
	if err != nil {
		var zero T
		return zero, err
	}

	m.frames[victimFrameID] = frame[T]{
		Page:     page,
		PinCount: 1,
		PageIdent: common.PageIdentity{
			FileID: pIdent.FileID,
			PageID: pIdent.PageID,
		},
	}

	m.pageToFrameInfo[pIdent] = victimFrameID

	m.pin(pIdent)

	return page, nil
}

func (m *Manager) reserveFrame() uint64 {
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

func (m *Manager) FlushPage(pIdent common.PageIdentity) error {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	frameID, ok := m.pageToFrameInfo[pIdent]
	if !ok {
		return fmt.Errorf("no frame for such page: %v", pIdent)
	}

	frame := &m.frames[frameID]
	if !frame.Page.IsDirty() {
		return nil
	}

	err := m.diskManager.WritePage(frame.Page, frame.PageIdent)
	if err != nil {
		return fmt.Errorf("failed to write page to disk: %w", err)
	}

	delete(m.DirtyPageTable, pIdent)

	frame.Page.SetDirtiness(false)

	return nil
}

func (m *Manager) FlushAllPages() error {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	for i := range m.frames {
		frame := &m.frames[i]
		if frame.Page.IsDirty() {
			_ = m.diskManager.WritePage(frame.Page, frame.PageIdent)

			frame.Page.SetDirtiness(false)

			delete(m.DirtyPageTable, frame.PageIdent)
		}
	}

	return nil
}
