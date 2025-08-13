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

type frame[T Page] struct {
	Page      T
	Idx       uint64
	PinCount  int
	PageIdent common.PageIdentity
}

type BufferPool[T Page] interface {
	Unpin(common.PageIdentity) error
	GetPage(common.PageIdentity) (T, error)
	GetPageNoCreate(common.PageIdentity) (T, error)
	FlushPage(common.PageIdentity) error
}

type frameInfo struct {
	frameID uint64
	isDirty bool
}

type Manager[T Page] struct {
	poolSize    uint64
	pageToFrame map[common.PageIdentity]uint64
	frames      []frame[T]
	emptyFrames []uint64

	replacer Replacer

	diskManager    DiskManager[T]
	DirtyPageTable map[common.PageIdentity]common.LSN

	fastPath sync.Mutex
	slowPath sync.Mutex
}

var (
	_ BufferPool[Page] = &Manager[Page]{}
)

func New[T Page](
	poolSize uint64,
	replacer Replacer,
	diskManager DiskManager[T],
) (*Manager[T], error) {
	assert.Assert(poolSize > 0, "pool size must be greater than zero")

	emptyFrames := make([]uint64, poolSize)
	for i := range poolSize {
		emptyFrames[i] = uint64(i)
	}

	frames := make([]frame[T], poolSize)

	return &Manager[T]{
		poolSize:       poolSize,
		pageToFrame:    make(map[common.PageIdentity]uint64),
		frames:         frames,
		emptyFrames:    emptyFrames,
		replacer:       replacer,
		diskManager:    diskManager,
		DirtyPageTable: make(map[common.PageIdentity]common.LSN),
	}, nil
}

func (m *Manager[T]) Unpin(pIdent common.PageIdentity) error {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	frameID, ok := m.pageToFrame[pIdent]
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

func (m *Manager[T]) pin(pIdent common.PageIdentity) {
	frameID, ok := m.pageToFrame[pIdent]

	assert.Assert(ok, "no frame for page: %v", pIdent)

	m.frames[frameID].PinCount++
	m.replacer.Pin(frameID)
}

func (m *Manager[T]) GetPageNoCreate(pageID common.PageIdentity) (T, error) {
	panic("NOT IMPLEMENTED")
}

func (m *Manager[T]) GetPage(pIdent common.PageIdentity) (T, error) {
	m.fastPath.Lock()

	if frameID, ok := m.pageToFrame[pIdent]; ok {
		m.pin(pIdent)
		m.fastPath.Unlock()

		return m.frames[frameID].Page, nil
	}

	m.fastPath.Unlock()

	m.slowPath.Lock()
	defer m.slowPath.Unlock()

	m.fastPath.Lock()
	if frameID, ok := m.pageToFrame[pIdent]; ok {
		m.pin(pIdent)
		m.fastPath.Unlock()

		return m.frames[frameID].Page, nil
	}
	m.fastPath.Unlock()

	frameID := m.reserveFrame()
	if frameID != noFrame {
		page, err := m.diskManager.ReadPage(common.PageIdentity{
			FileID: pIdent.FileID,
			PageID: pIdent.PageID,
		})
		if err != nil {
			var zero T
			return zero, err
		}

		m.frames[frameID] = frame[T]{
			Page:     page,
			PinCount: 1,
			PageIdent: common.PageIdentity{
				FileID: pIdent.FileID,
				PageID: pIdent.PageID,
			},
		}
		m.pageToFrame[pIdent] = frameID

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

	delete(m.pageToFrame, victimFrame.PageIdent)

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

	m.pageToFrame[pIdent] = victimFrameID

	m.pin(pIdent)

	return page, nil
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

func (m *Manager[T]) FlushPage(pIdent common.PageIdentity) error {
	m.fastPath.Lock()
	defer m.fastPath.Unlock()

	frameID, ok := m.pageToFrame[pIdent]
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

func (m *Manager[T]) FlushAllPages() error {
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
