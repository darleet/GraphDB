package page

import (
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	assert "github.com/Blackdeer1524/GraphDB/src/pkg/assert"
)

var (
	ErrNoEnoughSpace = errors.New("not enough space")
	ErrInvalidSlotID = errors.New("invalid slot ID")
)

// TODO добавить рисунок - иллюстрацию

const (
	Size       = 4096
	HeaderSize = uint32(unsafe.Sizeof(uint32(0)) * 3) // numSlots (4) + freeStart (4) + freeEnd (4)
	SlotSize   = uint32(unsafe.Sizeof(uint16(0)) * 2) // offset (2) + length (2)
)

type SlottedPage struct {
	data []byte

	locked atomic.Bool
	latch  sync.RWMutex
}

func NewSlottedPage() *SlottedPage {
	p := &SlottedPage{
		data: make([]byte, Size),
	}

	p.setNumSlots(0)
	p.setFreeStart(HeaderSize)
	p.setFreeEnd(Size)

	return p
}

func (p *SlottedPage) NumSlots() uint32 {
	return uint32(binary.LittleEndian.Uint32(p.data[0:4]))
}

func (p *SlottedPage) setNumSlots(n uint32) {
	binary.LittleEndian.PutUint32(p.data[0:4], uint32(n))
}

func (p *SlottedPage) freeStart() uint32 {
	return uint32(binary.LittleEndian.Uint32(p.data[4:8]))
}

func (p *SlottedPage) setFreeStart(n uint32) {
	binary.LittleEndian.PutUint32(p.data[4:8], uint32(n))
}

func (p *SlottedPage) freeEnd() uint32 {
	return uint32(binary.LittleEndian.Uint32(p.data[8:12]))
}

func (p *SlottedPage) setFreeEnd(n uint32) {
	binary.LittleEndian.PutUint32(p.data[8:12], uint32(n))
}

func (p *SlottedPage) getSlot(i uint32) (offset, length uint32) {
	base := HeaderSize + i*SlotSize

	offset = uint32(binary.LittleEndian.Uint16(p.data[base : base+2]))
	length = uint32(binary.LittleEndian.Uint16(p.data[base+2 : base+4]))

	return
}

func (p *SlottedPage) setSlot(i, offset, length uint32) {
	base := HeaderSize + i*SlotSize
	binary.LittleEndian.PutUint16(p.data[base:base+2], uint16(offset))
	binary.LittleEndian.PutUint16(p.data[base+2:base+4], uint16(length))
}

func (p *SlottedPage) Insert(record []byte) (uint32, error) {
	recLen := uint32(len(record))
	freeSpace := p.freeEnd() - p.freeStart()

	if freeSpace < recLen+SlotSize {
		return 0, ErrNoEnoughSpace
	}

	// Allocate space for the record
	newOffset := p.freeEnd() - recLen
	copy(p.data[newOffset:], record)

	// Create slot
	slotID := p.NumSlots()
	p.setSlot(slotID, newOffset, recLen)

	// Update header
	p.setNumSlots(slotID + 1)
	p.setFreeEnd(newOffset)
	p.setFreeStart(HeaderSize + (slotID+1)*SlotSize)

	return slotID, nil
}

func (p *SlottedPage) Get(slotID uint32) ([]byte, error) {
	if slotID >= p.NumSlots() {
		return nil, ErrInvalidSlotID
	}

	offset, length := p.getSlot(slotID)

	return p.data[offset : offset+length], nil
}

func (p *SlottedPage) GetData() []byte {
	assert.Assert(!p.locked.Load(), "GetData contract is violated")

	return p.data
}

func (p *SlottedPage) Lock() {
	p.latch.Lock()

	p.locked.Store(true)
}

func (p *SlottedPage) Unlock() {
	p.locked.Store(false)

	p.latch.Unlock()
}

func (p *SlottedPage) RLock() {
	p.latch.RLock()

	p.locked.Store(true)
}

func (p *SlottedPage) RUnlock() {
	p.locked.Store(false)

	p.latch.RUnlock()
}
