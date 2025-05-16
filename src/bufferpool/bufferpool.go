package bufferpool

import (
	"bytes"
	"encoding/binary"
	"errors"
)

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

type PageIdentity struct {
	FileID uint64
	PageID uint64
}

func (p PageIdentity) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, p.FileID)
	_ = binary.Write(buf, binary.BigEndian, p.PageID)
	return buf.Bytes(), nil
}

func (p *PageIdentity) UnmarshalBinary(data []byte) error {
	rd := bytes.NewReader(data)
	if err := binary.Read(rd, binary.BigEndian, &p.FileID); err != nil {
		return err
	}
	return binary.Read(rd, binary.BigEndian, &p.PageID)
}

type BufferPool[T Page] interface {
	Unpin(PageIdentity)
	GetPage(PageIdentity) (T, error)
	GetPageNoCreate(PageIdentity) (T, error)
	MarkDirty(PageIdentity) error
	FlushPage(PageIdentity) error
}
