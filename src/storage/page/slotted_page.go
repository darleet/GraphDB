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
	slotOffsetSize        = 12
	PageSize              = (1 << slotOffsetSize)
	slotOffsetMask uint16 = PageSize - 1
	slotPtrSize    uint16 = uint16(unsafe.Sizeof(slotPointer(1)))
)

type SlottedPage struct {
	data [PageSize]byte
}

type slotPointer uint16
type slotStatus byte

const (
	SlotStatusPrepareInsert slotStatus = iota
	SlotStatusInserted
	SlotStatusDeleted
)

func newSlotPtr(status slotStatus, recordOffset uint16) slotPointer {
	assert.Assert(recordOffset <= slotOffsetMask, "the offset is too big")
	return slotPointer((uint16(status) << slotOffsetSize) | recordOffset)
}

func (s slotPointer) RecordOffset() uint16 {
	return uint16(s) & slotOffsetMask
}

func (s slotPointer) RecordInfo() slotStatus {
	res := (uint16(s) & (^slotOffsetMask)) >> slotOffsetSize
	return slotStatus(res)
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

func (p *SlottedPage) InsertPrepare(data []byte) optional.Optional[uint16] {
	header := p.getHeader()
	// space required to store both the array and it's length
	requiredLength := int(unsafe.Sizeof(uint16(1))) + len(data)
	if int(header.freeEnd) < requiredLength {
		// uint16 overflow check
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

	ptrToLen := (*uint16)(unsafe.Pointer(&p.data[pos]))
	*ptrToLen = uint16(len(data))
	ptr := newSlotPtr(SlotStatusPrepareInsert, pos)

	dst := p.getBytesUnsafe(ptr)
	n := copy(dst, data)
	assert.Assert(
		n == len(data),
		"couldn't copy data. copied only %d bytes",
		len(data),
	)

	curSlot := header.slotsCount
	header.slotsCount++
	slots := header.getSlots()
	slots[curSlot] = ptr

	return optional.Some(curSlot)
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
		ptr.RecordInfo() == SlotStatusPrepareInsert,
		"tried to commit an insert to a wrong slot",
	)
	slots[slotHandle] = newSlotPtr(SlotStatusInserted, ptr.RecordOffset())
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
	data := p.Read(slotID)
	return dst.UnmarshalBinary(data)
}

func (p *SlottedPage) getBytesUnsafe(ptr slotPointer) []byte {
	offset := ptr.RecordOffset()
	sliceLen := *(*uint16)(unsafe.Pointer(&p.data[offset]))
	data := unsafe.Slice(
		&p.data[offset+uint16(unsafe.Sizeof(uint16(0)))],
		sliceLen,
	)
	return data
}

func (p *SlottedPage) assertSlotInserted(slotID uint16) slotPointer {
	header := p.getHeader()
	assert.Assert(slotID < header.slotsCount, "slotID is too large")
	ptr := header.getSlots()[slotID]
	assert.Assert(
		ptr.RecordInfo() == SlotStatusInserted,
		"tried to read from a slot with status %d", ptr.RecordInfo(),
	)
	return ptr
}

func (p *SlottedPage) Read(slotID uint16) []byte {
	ptr := p.assertSlotInserted(slotID)
	return p.getBytesUnsafe(ptr)
}

func (p *SlottedPage) Delete(slotID uint16) {
	ptr := p.assertSlotInserted(slotID)
	p.getHeader().getSlots()[slotID] = newSlotPtr(
		SlotStatusDeleted,
		ptr.RecordOffset(),
	)
}

func (p *SlottedPage) UnsafeRead(slotNumber uint16) []byte {
	assert.Assert(slotNumber < p.NumSlots(), "slotNumber is too large")

	header := p.getHeader()
	ptr := header.getSlots()[slotNumber]
	return p.getBytesUnsafe(ptr)
}

func (p *SlottedPage) UnsafeOverrideSlotStatus(
	slotNumber uint16,
	newStatus slotStatus,
) {
	assert.Assert(slotNumber < p.NumSlots(), "slotNumber is too large")

	header := p.getHeader()
	slot := header.getSlots()[slotNumber]

	header.getSlots()[slotNumber] = newSlotPtr(newStatus, slot.RecordOffset())
}

func (p *SlottedPage) UndoDelete(slotID uint16) {
	header := p.getHeader()
	assert.Assert(slotID < header.slotsCount, "slotID is too large")
	ptr := header.getSlots()[slotID]
	assert.Assert(
		ptr.RecordInfo() == SlotStatusDeleted,
		"tried to UndoDelete from a slot with status %d", ptr.RecordInfo(),
	)

	p.UnsafeOverrideSlotStatus(slotID, SlotStatusInserted)
}

func (p *SlottedPage) Update(slotID uint16, newData []byte) {
	data := p.Read(slotID)
	assert.Assert(len(data) >= len(newData))

	clear(data)
	copy(data, newData)
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
