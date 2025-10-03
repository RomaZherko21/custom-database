package buffer_bool

import (
	"custom-database/internal/disk_manager"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewLRUKCache(t *testing.T) {
	t.Run("1. New LRU-K cache creation success", func(t *testing.T) {
		// Arrange
		k := 2
		maxSize := 100

		// Act
		cache := NewLRUKCache(k, maxSize)

		// Assert
		require.NotNil(t, cache)
		require.Equal(t, k, cache.K)
		require.Equal(t, maxSize, cache.MaxSize)
		require.Equal(t, 0, cache.CurrentSize)
		require.NotNil(t, cache.PageEntries)
		require.NotNil(t, cache.AccessHistory)
		require.NotNil(t, cache.HotList)
		require.NotNil(t, cache.ColdList)
	})

	t.Run("2. New LRU-K cache with zero parameters", func(t *testing.T) {
		// Arrange
		k := 0
		maxSize := 0

		// Act
		cache := NewLRUKCache(k, maxSize)

		// Assert
		require.NotNil(t, cache)
		require.Equal(t, 0, cache.K)
		require.Equal(t, 0, cache.MaxSize)
		require.Equal(t, 0, cache.CurrentSize)
	})
}

func TestLRUKCacheAccess(t *testing.T) {
	t.Run("1. Access new page - adds to cold list", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)
		pageID := disk_manager.PageID{PageNumber: 1}

		// Act
		cache.Access(pageID)

		// Assert
		require.Equal(t, 1, cache.CurrentSize)
		require.Equal(t, 1, cache.ColdList.Size)
		require.Equal(t, 0, cache.HotList.Size)

		entry, exists := cache.PageEntries[pageID]
		require.True(t, exists)
		require.Equal(t, 1, entry.AccessCount)
		require.False(t, entry.IsInHotList)
		require.Equal(t, pageID, entry.PageID)
	})

	t.Run("2. Access page multiple times - moves to hot list when K reached", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)
		pageID := disk_manager.PageID{PageNumber: 1}

		// Act - first access
		cache.Access(pageID)
		require.Equal(t, 1, cache.ColdList.Size)
		require.Equal(t, 0, cache.HotList.Size)

		// Act - second access (K=2 reached)
		cache.Access(pageID)

		// Assert
		require.Equal(t, 1, cache.CurrentSize)
		require.Equal(t, 0, cache.ColdList.Size)
		require.Equal(t, 1, cache.HotList.Size)

		entry := cache.PageEntries[pageID]
		require.Equal(t, 2, entry.AccessCount)
		require.True(t, entry.IsInHotList)
	})

	t.Run("3. Access page in hot list - updates position", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)
		pageID1 := disk_manager.PageID{PageNumber: 1}
		pageID2 := disk_manager.PageID{PageNumber: 2}

		// Act - make both pages hot
		cache.Access(pageID1)
		cache.Access(pageID1) // pageID1 becomes hot
		cache.Access(pageID2)
		cache.Access(pageID2) // pageID2 becomes hot

		// Initially: [pageID2] <-> [pageID1] (pageID2 is head)
		require.Equal(t, pageID2, cache.HotList.Head.PageID)

		// Act - access pageID1 again
		cache.Access(pageID1)

		// Assert - pageID1 should move to head
		require.Equal(t, pageID1, cache.HotList.Head.PageID)
		require.Equal(t, pageID2, cache.HotList.Head.Prev.PageID)
	})

	t.Run("4. Access multiple different pages", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(3, 10)
		pageID1 := disk_manager.PageID{PageNumber: 1}
		pageID2 := disk_manager.PageID{PageNumber: 2}
		pageID3 := disk_manager.PageID{PageNumber: 3}

		// Act
		cache.Access(pageID1)
		cache.Access(pageID2)
		cache.Access(pageID3)

		// Assert
		require.Equal(t, 3, cache.CurrentSize)
		require.Equal(t, 3, cache.ColdList.Size)
		require.Equal(t, 0, cache.HotList.Size)

		// All pages should be in cold list (less than K=3 accesses)
		require.True(t, cache.PageEntries[pageID1].AccessCount < cache.K)
		require.True(t, cache.PageEntries[pageID2].AccessCount < cache.K)
		require.True(t, cache.PageEntries[pageID3].AccessCount < cache.K)
	})
}

func TestLRUKCacheEvict(t *testing.T) {
	t.Run("1. Evict existing page from cold list", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)
		pageID := disk_manager.PageID{PageNumber: 1}
		cache.Access(pageID)

		// Act
		result := cache.Evict(pageID)

		// Assert
		require.True(t, result)
		require.Equal(t, 0, cache.CurrentSize)
		require.Equal(t, 0, cache.ColdList.Size)
		require.Equal(t, 0, cache.HotList.Size)

		_, exists := cache.PageEntries[pageID]
		require.False(t, exists)

		_, exists = cache.AccessHistory[pageID]
		require.False(t, exists)
	})

	t.Run("2. Evict existing page from hot list", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)
		pageID := disk_manager.PageID{PageNumber: 1}
		cache.Access(pageID)
		cache.Access(pageID) // Make it hot

		// Act
		result := cache.Evict(pageID)

		// Assert
		require.True(t, result)
		require.Equal(t, 0, cache.CurrentSize)
		require.Equal(t, 0, cache.HotList.Size)
	})

	t.Run("3. Evict non-existing page", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)
		pageID := disk_manager.PageID{PageNumber: 999}

		// Act
		result := cache.Evict(pageID)

		// Assert
		require.False(t, result)
		require.Equal(t, 0, cache.CurrentSize)
	})

	t.Run("4. Evict page from middle of list", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)
		pageID1 := disk_manager.PageID{PageNumber: 1}
		pageID2 := disk_manager.PageID{PageNumber: 2}
		pageID3 := disk_manager.PageID{PageNumber: 3}

		cache.Access(pageID1)
		cache.Access(pageID2)
		cache.Access(pageID3)
		// Cold list: [pageID3] <-> [pageID2] <-> [pageID1]

		// Act
		result := cache.Evict(pageID2)

		// Assert
		require.True(t, result)
		require.Equal(t, 2, cache.CurrentSize)
		require.Equal(t, 2, cache.ColdList.Size)

		// Check that pageID2 is removed and list is still connected
		require.Equal(t, pageID3, cache.ColdList.Head.PageID)
		require.Equal(t, pageID1, cache.ColdList.Tail.PageID)
	})
}

func TestLRUKCacheGetVictim(t *testing.T) {
	t.Run("1. Get victim from cold list", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)
		pageID1 := disk_manager.PageID{PageNumber: 1}
		pageID2 := disk_manager.PageID{PageNumber: 2}

		cache.Access(pageID1)
		cache.Access(pageID2)
		// Cold list: [pageID2] <-> [pageID1] (pageID1 is tail)

		// Act
		victim := cache.GetVictim(nil)

		// Assert
		require.Equal(t, pageID1, victim)
	})

	t.Run("2. Get victim from hot list when cold list is empty", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)
		pageID1 := disk_manager.PageID{PageNumber: 1}
		pageID2 := disk_manager.PageID{PageNumber: 2}

		cache.Access(pageID1)
		cache.Access(pageID1) // Make hot
		cache.Access(pageID2)
		cache.Access(pageID2) // Make hot
		// Hot list: [pageID2] <-> [pageID1] (pageID1 is tail)

		// Act
		victim := cache.GetVictim(nil)

		// Assert
		require.Equal(t, pageID1, victim)
	})

	t.Run("3. Get victim from empty cache", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)

		// Act
		victim := cache.GetVictim(nil)

		// Assert
		require.Equal(t, disk_manager.PageID{}, victim)
	})

	t.Run("4. Get victim prioritizes cold list over hot list", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)
		pageID1 := disk_manager.PageID{PageNumber: 1} // Will be hot
		pageID2 := disk_manager.PageID{PageNumber: 2} // Will be cold

		cache.Access(pageID1)
		cache.Access(pageID1) // Make hot
		cache.Access(pageID2) // Still cold

		// Act
		victim := cache.GetVictim(nil)

		// Assert - should get from cold list, not hot list
		require.Equal(t, pageID2, victim)
	})
}

func TestLRUKCacheComplexScenario(t *testing.T) {
	t.Run("1. Complex LRU-K scenario with multiple pages", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 5)
		pageID1 := disk_manager.PageID{PageNumber: 1}
		pageID2 := disk_manager.PageID{PageNumber: 2}
		pageID3 := disk_manager.PageID{PageNumber: 3}
		pageID4 := disk_manager.PageID{PageNumber: 4}
		pageID5 := disk_manager.PageID{PageNumber: 5}

		// Act - Access pattern: 1,2,1,3,2,4,5,1,2,3
		cache.Access(pageID1) // Cold: [1]
		cache.Access(pageID2) // Cold: [2,1]
		cache.Access(pageID1) // Cold: [1,2] (pageID1 moves to head)
		cache.Access(pageID3) // Cold: [3,1,2]
		cache.Access(pageID2) // Hot: [2], Cold: [3,1] (pageID2 becomes hot)
		cache.Access(pageID4) // Hot: [2], Cold: [4,3,1]
		cache.Access(pageID5) // Hot: [2], Cold: [5,4,3,1]
		cache.Access(pageID1) // Hot: [1,2], Cold: [5,4,3] (pageID1 becomes hot)
		cache.Access(pageID2) // Hot: [2,1], Cold: [5,4,3] (pageID2 moves to head)
		cache.Access(pageID3) // Hot: [3,2,1], Cold: [5,4] (pageID3 becomes hot)

		// Assert
		require.Equal(t, 5, cache.CurrentSize)
		require.Equal(t, 3, cache.HotList.Size)
		require.Equal(t, 2, cache.ColdList.Size)

		// Check hot list order: [3,2,1] (Head слева направо)
		require.Equal(t, pageID3, cache.HotList.Head.PageID)
		require.Equal(t, pageID2, cache.HotList.Head.Prev.PageID)
		require.Equal(t, pageID1, cache.HotList.Head.Prev.Prev.PageID)

		// Check cold list order: [5,4] (Head слева направо)
		require.Equal(t, pageID5, cache.ColdList.Head.PageID)
		require.Equal(t, pageID4, cache.ColdList.Head.Prev.PageID)

		// Get victim should return pageID4 (tail of cold list)
		victim := cache.GetVictim(nil)
		require.Equal(t, pageID4, victim)
	})
}

func TestLRUKCacheAccessHistory(t *testing.T) {
	t.Run("1. Access history is maintained correctly", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(3, 10)
		pageID := disk_manager.PageID{PageNumber: 1}

		// Act - Access page 4 times
		cache.Access(pageID)
		cache.Access(pageID)
		cache.Access(pageID)
		cache.Access(pageID)

		// Assert
		history, exists := cache.AccessHistory[pageID]
		require.True(t, exists)
		require.Equal(t, 3, history.Count) // Should keep only last K=3 accesses
		require.Len(t, history.Accesses, 3)
	})

	t.Run("2. Access history timestamps are ordered", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(3, 10)
		pageID := disk_manager.PageID{PageNumber: 1}

		// Act
		cache.Access(pageID)
		time.Sleep(1 * time.Millisecond)

		cache.Access(pageID)
		time.Sleep(1 * time.Millisecond)

		cache.Access(pageID)

		// Assert
		history := cache.AccessHistory[pageID]
		require.Equal(t, 3, history.Count)

		// Timestamps should be in ascending order (oldest to newest)
		require.True(t, history.Accesses[0].Before(history.Accesses[1]))
		require.True(t, history.Accesses[1].Before(history.Accesses[2]))
	})
}

func TestLRUKCacheGetVictimWithPinCheck(t *testing.T) {
	t.Run("1. Get victim with no pinned pages", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)
		pageID1 := disk_manager.PageID{PageNumber: 1}
		pageID2 := disk_manager.PageID{PageNumber: 2}

		cache.Access(pageID1)
		cache.Access(pageID2)
		// Cold list: [pageID2] <-> [pageID1] (pageID1 is tail)

		// Функция, которая всегда возвращает false (нет закрепленных страниц)
		isPinned := func(pageID disk_manager.PageID) bool {
			return false
		}

		// Act
		victim := cache.GetVictim(isPinned)

		// Assert - должен вернуть самую старую страницу
		require.Equal(t, pageID1, victim)
	})

	t.Run("2. Get victim skipping pinned pages in cold list", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)
		pageID1 := disk_manager.PageID{PageNumber: 1}
		pageID2 := disk_manager.PageID{PageNumber: 2}
		pageID3 := disk_manager.PageID{PageNumber: 3}

		cache.Access(pageID1)
		cache.Access(pageID2)
		cache.Access(pageID3)
		// Cold list: [pageID3] <-> [pageID2] <-> [pageID1] (pageID1 is tail)

		// pageID1 закреплена, pageID2 и pageID3 не закреплены
		isPinned := func(pageID disk_manager.PageID) bool {
			return pageID == pageID1
		}

		// Act
		victim := cache.GetVictim(isPinned)

		// Assert - должен вернуть pageID2 (следующая после pageID1)
		require.Equal(t, pageID2, victim)
	})

	t.Run("3. Get victim from hot list when cold list pages are pinned", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)
		pageID1 := disk_manager.PageID{PageNumber: 1} // Will be cold
		pageID2 := disk_manager.PageID{PageNumber: 2} // Will be hot
		pageID3 := disk_manager.PageID{PageNumber: 3} // Will be hot

		cache.Access(pageID1) // Cold
		cache.Access(pageID2)
		cache.Access(pageID2) // Hot
		cache.Access(pageID3)
		cache.Access(pageID3) // Hot
		// Cold list: [pageID1], Hot list: [pageID3] <-> [pageID2]

		// pageID1 закреплена, pageID2 и pageID3 не закреплены
		isPinned := func(pageID disk_manager.PageID) bool {
			return pageID == pageID1
		}

		// Act
		victim := cache.GetVictim(isPinned)

		// Assert - должен вернуть pageID2 (tail of hot list)
		require.Equal(t, pageID2, victim)
	})

	t.Run("4. Get victim when all pages are pinned", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)
		pageID1 := disk_manager.PageID{PageNumber: 1}
		pageID2 := disk_manager.PageID{PageNumber: 2}

		cache.Access(pageID1)
		cache.Access(pageID2)

		// Все страницы закреплены
		isPinned := func(pageID disk_manager.PageID) bool {
			return true
		}

		// Act
		victim := cache.GetVictim(isPinned)

		// Assert - должен вернуть пустой PageID
		require.Equal(t, disk_manager.PageID{}, victim)
	})

	t.Run("5. Get victim with mixed pinned/unpinned pages in hot list", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)
		pageID1 := disk_manager.PageID{PageNumber: 1} // Will be hot
		pageID2 := disk_manager.PageID{PageNumber: 2} // Will be hot
		pageID3 := disk_manager.PageID{PageNumber: 3} // Will be hot

		cache.Access(pageID1)
		cache.Access(pageID1) // Hot
		cache.Access(pageID2)
		cache.Access(pageID2) // Hot
		cache.Access(pageID3)
		cache.Access(pageID3) // Hot
		// Hot list: [pageID3] <-> [pageID2] <-> [pageID1] (pageID1 is tail)

		// pageID1 закреплена, pageID2 и pageID3 не закреплены
		isPinned := func(pageID disk_manager.PageID) bool {
			return pageID == pageID1
		}

		// Act
		victim := cache.GetVictim(isPinned)

		// Assert - должен вернуть pageID2 (следующая после pageID1)
		require.Equal(t, pageID2, victim)
	})

	t.Run("6. Get victim from empty cache", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)
		isPinned := func(pageID disk_manager.PageID) bool {
			return false
		}

		// Act
		victim := cache.GetVictim(isPinned)

		// Assert
		require.Equal(t, disk_manager.PageID{}, victim)
	})

	t.Run("7. Get victim prioritizes cold list over hot list", func(t *testing.T) {
		// Arrange
		cache := NewLRUKCache(2, 10)
		pageID1 := disk_manager.PageID{PageNumber: 1} // Will be hot
		pageID2 := disk_manager.PageID{PageNumber: 2} // Will be cold

		cache.Access(pageID1)
		cache.Access(pageID1) // Make hot
		cache.Access(pageID2) // Still cold
		// Hot list: [pageID1], Cold list: [pageID2]

		// Ни одна страница не закреплена
		isPinned := func(pageID disk_manager.PageID) bool {
			return false
		}

		// Act
		victim := cache.GetVictim(isPinned)

		// Assert - должен вернуть из cold list, не из hot list
		require.Equal(t, pageID2, victim)
	})
}
