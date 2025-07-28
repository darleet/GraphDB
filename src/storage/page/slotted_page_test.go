package page

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/Blackdeer1524/GraphDB/src/pkg/optional"
)

func TestInsertAndGet(t *testing.T) {
	page := NewSlottedPage()

	records := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
	}

	var slotIDs []uint16

	for _, rec := range records {
		slot := page.InsertPrepare(rec)
		require.NotEqual(t, slot, None[uint16]())
		page.InsertCommit(slot.Unwrap())
		slotIDs = append(slotIDs, slot.Unwrap())
	}

	for i, id := range slotIDs {
		got := page.GetBytes(id)
		assert.Equal(
			t,
			string(records[i]),
			string(got),
			"Retrieved record doesn't match",
		)
	}
}

func TestInsertAndGetLarger(t *testing.T) {
	page := NewSlottedPage()
	i := 0
	for {
		handle := page.InsertPrepare([]byte(strconv.Itoa(i)))
		if handle.IsNone() {
			break
		}
		page.InsertCommit(handle.Unwrap())
		i++
	}

	for j := range i {
		data := page.GetBytes(uint16(j))
		expected := []byte(strconv.Itoa(j))
		assert.Equal(t, expected, data)
	}
}

func TestFreeSpaceReduction(t *testing.T) {
	page := NewSlottedPage()
	initialFree := page.getHeader().freeEnd - page.getHeader().freeStart

	slot := page.InsertPrepare([]byte("1234567890"))
	page.InsertCommit(slot.Unwrap())

	used := page.getHeader().freeEnd - page.getHeader().freeStart
	assert.Less(t, used, initialFree, "Free space did not reduce correctly")
}

func TestInsertTooLarge(t *testing.T) {
	page := NewSlottedPage()

	tooBig := make([]byte, PageSize)
	handle := page.InsertPrepare(tooBig)
	assert.Equal(t, None[uint16](), handle)
}

func TestInvalidSlotID(t *testing.T) {
	page := NewSlottedPage()
	assert.Panicsf(t,
		func() {
			_ = page.GetBytes(uint16(999))
		},
		"123",
	)
}
