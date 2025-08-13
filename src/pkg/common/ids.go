package common

import (
	"bytes"
	"encoding/binary"
)

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

type FileLocation struct {
	PageID  uint64
	SlotNum uint16
}

type RecordID struct {
	FileID  uint64
	PageID  uint64
	SlotNum uint16
}

func (r RecordID) PageIdentity() PageIdentity {
	return PageIdentity{
		FileID: r.FileID,
		PageID: r.PageID,
	}
}

func (r RecordID) FileLocation() FileLocation {
	return FileLocation{
		PageID:  r.PageID,
		SlotNum: r.SlotNum,
	}
}
