package page

import (
	"encoding"
	"sync"
	"unsafe"

	assert "github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/optional"
)

// TODO добавить рисунок - иллюстрацию

const (
	PageSize = 4096
)

type SlottedPage struct {
	data [PageSize]byte
}

type slotPointer uint16
type slotInfo byte

const (
	slotStatusFree slotInfo = iota
	slotStatusPrepareInsert
	slotStatusInserted
	slotStatusDeleted
)

const (
	slotOffsetSize        = 12
	slotOffsetMask uint16 = (1 << slotOffsetSize) - 1
	slotPtrSize    uint16 = uint16(unsafe.Sizeof(slotPointer(1)))
)

func newSlotPtr(info slotInfo, offset uint16) slotPointer {
	assert.Assert(offset <= slotOffsetMask, "the offset is too big")
	return slotPointer((uint16(info) << slotOffsetSize) | offset)
}

func (s slotPointer) recordOffset() uint16 {
	return uint16(s) & slotOffsetMask
}

func (s slotPointer) recordInfo() slotInfo {
	res := (uint16(s) & (^slotOffsetMask)) >> slotOffsetSize
	return slotInfo(res)
}

type header struct {
	latch sync.RWMutex

	dirty bool

	freeStart uint16
	freeEnd   uint16

	slotsCount uint16
	slots      slotPointer
}

func (h *header) getSlots() []slotPointer {
	return unsafe.Slice(&h.slots, h.slotsCount)
}

func (p *SlottedPage) getHeader() *header {
	return (*header)(unsafe.Pointer(&p.data[0]))
}

func InsertSerializable[T encoding.BinaryMarshaler](
	p *SlottedPage,
	obj T,
) optional.Optional[uint16] {
	bytes, err := obj.MarshalBinary()
	assert.Assert(err != nil)
	return p.InsertPrepare(bytes)
}

func (p *SlottedPage) InsertCommit(slotHandle uint16) {
	header := p.getHeader()
	assert.Assert(
		uint16(slotHandle) < header.slotsCount,
		"slot number is too large. actual: %d. slots count: %d",
		slotHandle,
		header.slotsCount,
	)

	slots := header.getSlots()
	ptr := slots[slotHandle]
	assert.Assert(
		ptr.recordInfo() == slotStatusPrepareInsert,
		"tried to commit an insert to a wrong slot",
	)
	slots[slotHandle] = newSlotPtr(slotStatusInserted, ptr.recordOffset())
}

func (p *SlottedPage) InsertPrepare(data []byte) optional.Optional[uint16] {
	header := p.getHeader()
	// space required to store both the array and it's length
	requiredLength := int(unsafe.Sizeof(int(1))) + len(data)
	if header.freeEnd < uint16(requiredLength) {
		return optional.None[uint16]()
	}

	pos := header.freeEnd - uint16(requiredLength)
	if pos < header.freeStart+slotPtrSize {
		return optional.None[uint16]()
	}
	defer func() {
		header.freeStart += slotPtrSize
		header.freeEnd = pos
	}()

	ptrToLen := (*int)(unsafe.Pointer(&p.data[pos]))
	*ptrToLen = len(data)

	dst := unsafe.Slice(&p.data[pos+uint16(unsafe.Sizeof(int(1)))], len(data))
	n := copy(dst, data)
	assert.Assert(
		n == int(len(data)),
		"couldn't copy data. copied only %d bytes",
		len(data),
	)

	curSlot := header.slotsCount
	header.slotsCount++

	slots := header.getSlots()
	slots[curSlot] = newSlotPtr(slotStatusPrepareInsert, pos)
	return optional.Some(curSlot)
}

func NewSlottedPage() *SlottedPage {
	p := &SlottedPage{
		data: [PageSize]byte{},
	}
	head := p.getHeader()
	head.freeStart = uint16(unsafe.Sizeof(header{}))
	head.freeEnd = PageSize
	return p
}

func (p *SlottedPage) NumSlots() uint16 {
	header := p.getHeader()
	return header.slotsCount
}

func Get[T encoding.BinaryUnmarshaler](
	p *SlottedPage,
	slotID uint16,
	dst T,
) error {
	data := p.GetBytes(slotID)
	return dst.UnmarshalBinary(data)
}

func (p *SlottedPage) GetBytes(slotID uint16) []byte {
	header := p.getHeader()

	assert.Assert(slotID < p.NumSlots(), "invalid slotID")
	assert.Assert(slotID < header.slotsCount, "slotID is too large")

	ptr := header.getSlots()[slotID]
	assert.Assert(
		ptr.recordInfo() == slotStatusInserted,
		"tried to read from a slot with status %d", ptr.recordInfo(),
	)

	offset := ptr.recordOffset()
	sliceLen := *(*int)(unsafe.Pointer(&p.data[offset]))
	data := unsafe.Slice(
		&p.data[offset+uint16(unsafe.Sizeof(int(0)))],
		sliceLen,
	)
	return data
}

func (p *SlottedPage) Lock() {
	p.getHeader().latch.Lock()
}

func (p *SlottedPage) Unlock() {
	p.getHeader().latch.Unlock()
}

func (p *SlottedPage) RLock() {
	p.getHeader().latch.RLock()
}

func (p *SlottedPage) RUnlock() {
	p.getHeader().latch.RUnlock()
}

func (p *SlottedPage) SetDirtiness(val bool) {
	p.getHeader().dirty = val
}

func (p *SlottedPage) IsDirty() bool {
	return p.getHeader().dirty
}

func (p *SlottedPage) GetData() []byte {
	return p.data[:]
}

func (p *SlottedPage) SetData(data []byte) {
	copy(p.data[:], data)
}
