package page

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		handle := page.PrepareInsertBytes(rec)
		require.NotEqual(t, handle, INVALID_SLOT_NUMBER)
		slot := page.CommitInsert(handle)
		slotIDs = append(slotIDs, slot)
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
		handle := page.PrepareInsertBytes([]byte(strconv.Itoa(i)))
		if handle == INVALID_SLOT_NUMBER {
			break
		}
		page.CommitInsert(handle)
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

	handle := page.PrepareInsertBytes([]byte("1234567890"))
	_ = page.CommitInsert(handle)

	used := page.getHeader().freeEnd - page.getHeader().freeStart
	assert.Less(t, used, initialFree, "Free space did not reduce correctly")
}

func TestInsertTooLarge(t *testing.T) {
	page := NewSlottedPage()

	tooBig := make([]byte, PageSize)
	handle := page.PrepareInsertBytes(tooBig)
	assert.Equal(t, INVALID_SLOT_NUMBER, handle)
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
