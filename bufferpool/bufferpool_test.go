package bufferpool_test

import (
	"testing"

	"github.com/Blackdeer1524/GraphDB/bufferpool"
	"github.com/Blackdeer1524/GraphDB/bufferpool/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPage_CacheMissAndLoad(t *testing.T) {
	replacer := mocks.NewMockReplacer()
	disk := mocks.NewMockDiskManager()

	page := &mocks.MockPage{Data: []byte("hello")}

	disk.Pages[[2]uint64{1, 1}] = page

	mgr, err := bufferpool.New(2, replacer, disk)
	require.NoError(t, err)

	p, err := mgr.GetPage(1, 1)

	require.NoError(t, err)

	require.NotNil(t, p)
	assert.Equal(t, "hello", string(p.GetData()))
}

func TestUnpin(t *testing.T) {
	replacer := mocks.NewMockReplacer()
	disk := mocks.NewMockDiskManager()

	page := &mocks.MockPage{Data: []byte("page")}
	disk.Pages[[2]uint64{2, 2}] = page

	mgr, _ := bufferpool.New(2, replacer, disk)

	_, err := mgr.GetPage(2, 2)
	require.NoError(t, err)

	err = mgr.Unpin(2, 2)
	require.NoError(t, err)
	assert.Equal(t, true, !replacer.Pinned[0])
}

func TestMarkDirtyAndFlushPage(t *testing.T) {
	replacer := mocks.NewMockReplacer()
	disk := mocks.NewMockDiskManager()
	page := &mocks.MockPage{Data: []byte("dirty")}
	disk.Pages[[2]uint64{3, 3}] = page

	mgr, _ := bufferpool.New(2, replacer, disk)

	_, err := mgr.GetPage(3, 3)
	require.NoError(t, err)

	err = mgr.MarkDirty(3, 3)
	require.NoError(t, err)

	err = mgr.FlushPage(3, 3)
	require.NoError(t, err)
}

func TestFlushAllPages(t *testing.T) {
	replacer := mocks.NewMockReplacer()
	disk := mocks.NewMockDiskManager()

	page := &mocks.MockPage{Data: []byte("dirty")}
	disk.Pages = map[[2]uint64]*mocks.MockPage{
		{4, 4}: page,
	}

	mgr, _ := bufferpool.New(2, replacer, disk)

	_, err := mgr.GetPage(4, 4)
	require.NoError(t, err)
	_ = mgr.MarkDirty(4, 4)

	err = mgr.FlushAllPages()
	require.NoError(t, err)
}

func TestChooseVictim(t *testing.T) {
	replacer := mocks.NewMockReplacer()
	disk := mocks.NewMockDiskManager()

	page := &mocks.MockPage{Data: []byte("first")}

	disk.Pages = map[[2]uint64]*mocks.MockPage{
		{5, 5}: page,
		{5, 6}: {Data: []byte("second")},
	}

	mgr, _ := bufferpool.New(1, replacer, disk)

	_, _ = mgr.GetPage(5, 5)
	_ = mgr.Unpin(5, 5)
	_, err := mgr.GetPage(5, 6)
	require.NoError(t, err)
}
