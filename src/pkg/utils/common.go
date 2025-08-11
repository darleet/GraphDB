package utils

import (
	"bytes"
	"encoding/binary"
)

func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}

	return v
}

type Pair[T, K any] struct {
	First  T
	Second K
}

func (p Pair[T, K]) Destruct() (T, K) {
	return p.First, p.Second
}

func Uint32ToBytes(num uint32) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, num)
	if err != nil {
		panic(err) // Handle error appropriately
	}
	return buf.Bytes()
}

func BytesToUint32(b []byte) uint32 {
	var num uint32
	buf := bytes.NewReader(b)
	err := binary.Read(buf, binary.BigEndian, &num)
	if err != nil {
		panic(err) // Handle error appropriately
	}
	return num
}
