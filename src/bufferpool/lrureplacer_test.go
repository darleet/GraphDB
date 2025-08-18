package bufferpool

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

func TestLRUReplacerBasic(t *testing.T) {
	r := NewLRUReplacer()

	fileID := common.FileID(42)
	first := common.PageIdentity{FileID: fileID, PageID: 1}
	second := common.PageIdentity{FileID: fileID, PageID: 2}
	third := common.PageIdentity{FileID: fileID, PageID: 3}
	fourth := common.PageIdentity{FileID: fileID, PageID: 4}
	fifth := common.PageIdentity{FileID: fileID, PageID: 5}

	r.Unpin(first)
	r.Unpin(second)
	r.Unpin(third)

	assert.Equal(t, uint64(3), r.GetSize())

	r.Pin(second)
	assert.Equal(t, uint64(2), r.GetSize())

	victim, err := r.ChooseVictim()
	assert.NoError(t, err)
	assert.Equal(t, first, victim)

	assert.Equal(t, uint64(1), r.GetSize())

	r.Unpin(fourth)
	r.Unpin(fifth)

	assert.Equal(t, uint64(3), r.GetSize())

	v1, _ := r.ChooseVictim()
	v2, _ := r.ChooseVictim()

	assert.ElementsMatch(
		t,
		[]common.PageIdentity{third, fourth},
		[]common.PageIdentity{v1, v2},
	)

	assert.Equal(t, uint64(1), r.GetSize())
}

func TestLRUChooseVictimEmpty(t *testing.T) {
	r := NewLRUReplacer()

	_, err := r.ChooseVictim()
	assert.Error(t, err)
}
