package common

import (
	"bytes"
	"encoding/binary"
)

type LSN uint64

var NIL_LSN LSN = LSN(0)

// is considered NIL iff lsn is NIL_LSN
type LogRecordLocationInfo struct {
	Lsn      LSN
	Location FileLocation
}

func NewNilLogRecordLocation() LogRecordLocationInfo {
	return LogRecordLocationInfo{
		Lsn:      NIL_LSN,
		Location: FileLocation{},
	}
}

func (p *LogRecordLocationInfo) IsNil() bool {
	return p.Lsn == NIL_LSN
}

func (l *LogRecordLocationInfo) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, l.Lsn); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, l.Location.PageID); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, l.Location.SlotNum); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (l *LogRecordLocationInfo) UnmarshalBinary(data []byte) error {
	rd := bytes.NewReader(data)
	if err := binary.Read(rd, binary.BigEndian, &l.Lsn); err != nil {
		return err
	}

	if err := binary.Read(rd, binary.BigEndian, &l.Location.PageID); err != nil {
		return err
	}

	if err := binary.Read(rd, binary.BigEndian, &l.Location.SlotNum); err != nil {
		return err
	}

	return nil
}
