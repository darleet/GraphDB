package bufferpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

func TestGetPage_Cached(t *testing.T) {
	mockDisk := new(MockDiskManager)
	mockReplacer := new(MockReplacer)

	manager, err := New(1, mockReplacer, mockDisk)
	require.NoError(t, err)

	fileID, pageID := uint64(1), uint64(0)

	p := NewSlottedPage_mock()
	p.SetData([]byte("cached data"))

	frameID := uint64(0)
	manager.frames[frameID] = frame[*SlottedPage_mock]{
		Page:     p,
		PinCount: 0,
		PageIdent: common.PageIdentity{
			FileID: fileID,
			PageID: pageID,
		},
	}
	manager.pageToFrame[common.PageIdentity{FileID: fileID, PageID: pageID}] = frameID

	mockReplacer.On("Pin", frameID).Return()

	pIdent := common.PageIdentity{FileID: fileID, PageID: pageID}

	result, err := manager.GetPage(pIdent)

	assert.NoError(t, err)
	assert.Equal(t, p, result)

	// не должно быть считывания с диска
	mockDisk.AssertNotCalled(t, "ReadPage", fileID, pageID)

	mockReplacer.AssertExpectations(t)
	mockReplacer.AssertNotCalled(t, "Unpin", mock.Anything)
}

func TestGetPage_LoadFromDisk(t *testing.T) {
	mockDisk := new(MockDiskManager)
	mockReplacer := new(MockReplacer)

	manager, err := New(1, mockReplacer, mockDisk)
	require.NoError(t, err)

	fileID, pageID := uint64(1), uint64(0)
	pageIdent := common.PageIdentity{
		FileID: fileID,
		PageID: pageID,
	}

	expectedPage := NewSlottedPage_mock()
	expectedPage.SetData([]byte("disk data"))

	mockDisk.On("ReadPage", pageIdent).Return(expectedPage, nil)
	mockReplacer.On("Pin", uint64(0)).Return()

	result, err := manager.GetPage(pageIdent)

	assert.NoError(t, err)
	assert.Equal(t, expectedPage, result)

	assert.Equal(t, uint64(0), manager.pageToFrame[pageIdent])
	assert.Equal(t, expectedPage, manager.frames[0].Page)

	mockDisk.AssertExpectations(t)
	mockReplacer.AssertExpectations(t)
}

func TestGetPage_LoadFromDisk_WithExistingPage(t *testing.T) {
	mockDisk := new(MockDiskManager)
	mockReplacer := new(MockReplacer)

	// Создаем пул из 2 фреймов
	manager, err := New(2, mockReplacer, mockDisk)
	require.NoError(t, err)

	existingFileID, existingPageID := uint64(1), uint64(0)
	existingPage := NewSlottedPage_mock()
	existingPage.SetData([]byte("existing data"))

	existingPageData := common.PageIdentity{
		FileID: existingFileID,
		PageID: existingPageID,
	}

	frameID := uint64(0)
	manager.frames[frameID] = frame[*SlottedPage_mock]{
		Page:      existingPage,
		PinCount:  1,
		PageIdent: existingPageData,
	}
	manager.pageToFrame[existingPageData] = frameID
	manager.emptyFrames = []uint64{1}

	newFileID := uint64(2)
	newPageID := uint64(1)
	newPage := NewSlottedPage_mock()
	newPage.SetData([]byte("new data"))

	pIdent := common.PageIdentity{FileID: newFileID, PageID: newPageID}
	mockDisk.On("ReadPage", pIdent).Return(newPage, nil)
	mockReplacer.On("Pin", uint64(1)).Return()

	result, err := manager.GetPage(pIdent)
	assert.NoError(t, err)
	assert.Equal(t, newPage, result)

	assert.Equal(t, uint64(1), manager.pageToFrame[pIdent])
	assert.Equal(t, newPage, manager.frames[1].Page)

	assert.Equal(t, existingPage, manager.frames[0].Page)
	assert.Equal(
		t,
		uint64(0),
		manager.pageToFrame[common.PageIdentity{FileID: existingFileID, PageID: existingPageID}],
	)

	mockDisk.AssertExpectations(t)
	mockReplacer.AssertExpectations(t)
}

func TestGetPage_LoadFromDisk_WithVictimReplacement(t *testing.T) {
	mockDisk := new(MockDiskManager)
	mockReplacer := new(MockReplacer)

	manager, err := New(1, mockReplacer, mockDisk)
	require.NoError(t, err)

	existingFileID, existingPageID := uint64(1), uint64(0)
	existingPage := NewSlottedPage_mock()
	existingPage.SetData([]byte("old data"))
	existingPage.SetDirtiness(true)

	existingPageIdent := common.PageIdentity{
		FileID: existingFileID,
		PageID: existingPageID,
	}

	frameID := uint64(0)
	manager.frames[frameID] = frame[*SlottedPage_mock]{
		Page:      existingPage,
		PinCount:  0,
		PageIdent: existingPageIdent,
	}
	manager.pageToFrame[common.PageIdentity{FileID: existingFileID, PageID: existingPageID}] = frameID
	manager.emptyFrames = nil

	newPage := NewSlottedPage_mock()
	newPage.SetData([]byte("new data"))

	mockReplacer.On("ChooseVictim").Return(frameID, nil)
	mockReplacer.On("Pin", frameID).Return()

	mockDisk.On("WritePage", existingPage, existingPageIdent).Return(nil)

	newFileID, newPageID := uint64(2), uint64(1)
	pIdent := common.PageIdentity{FileID: newFileID, PageID: newPageID}
	mockDisk.On("ReadPage", pIdent).Return(newPage, nil)

	result, err := manager.GetPage(pIdent)

	assert.NoError(t, err)
	assert.Equal(t, newPage, result)

	oldIdent := common.PageIdentity{
		FileID: existingFileID,
		PageID: existingPageID,
	}
	_, exists := manager.pageToFrame[oldIdent]
	assert.False(t, exists, "Старая страница не удалена из pageToFrame")

	assert.Equal(t, frameID, manager.pageToFrame[pIdent])
	assert.Equal(t, newPage, manager.frames[frameID].Page)

	mockReplacer.AssertExpectations(t)
	mockDisk.AssertExpectations(t)
}
