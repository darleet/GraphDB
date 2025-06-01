package bufferpool

import (
	"errors"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

var (
	_ BufferPool[*page.SlottedPage] = &BufferPool_mock{}
)

type BufferPool_mock struct {
	mu       sync.Mutex
	pages    map[PageIdentity]*page.SlottedPage
	unpinned map[PageIdentity]bool // tracks unpinned pages
}

func NewBufferPoolMock() *BufferPool_mock {
	return &BufferPool_mock{
		pages:    make(map[PageIdentity]*page.SlottedPage),
		unpinned: make(map[PageIdentity]bool),
	}
}

func (b *BufferPool_mock) Unpin(pageID PageIdentity) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.unpinned[pageID] = true
}

func (b *BufferPool_mock) GetPage(pageID PageIdentity) (*page.SlottedPage, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	p, ok := b.pages[pageID]
	if !ok {
		// Create a new page for the mock
		p = page.NewSlottedPage()
		b.pages[pageID] = p
	}
	b.unpinned[pageID] = false
	return p, nil
}

func (b *BufferPool_mock) GetPageNoCreate(pageID PageIdentity) (*page.SlottedPage, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	p, ok := b.pages[pageID]
	if !ok {
		return nil, ErrNoSuchPage
	}
	b.unpinned[pageID] = false
	return p, nil
}

func (b *BufferPool_mock) FlushPage(pageID PageIdentity) error {
	return nil
}

func (b *BufferPool_mock) EnsureAllPagesUnpinned() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for pageID := range b.pages {
		if !b.unpinned[pageID] {
			return errors.New("not all pages were unpinned")
		}
	}
	return nil
}
