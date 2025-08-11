package txns

import "testing"

func TestLocker(t *testing.T) {
	locker := NewLocker()
	locker.LockCatalog(1, GRANULAR_LOCK_SHARED)
}
