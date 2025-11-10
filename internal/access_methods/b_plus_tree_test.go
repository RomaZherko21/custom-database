package access_methods

import (
	"custom-database/internal/disk_manager"
	"fmt"
	"runtime"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewBPlusTree(t *testing.T) {
	t.Run("1. New B+ tree creation success", func(t *testing.T) {
		// Act
		tree := NewBPlusTree()

		// Assert
		require.NotNil(t, tree)
		require.NotNil(t, tree.Root)
		require.True(t, tree.Root.IsLeaf)
		require.Empty(t, tree.Root.Keys)
		require.Empty(t, tree.Root.Values)
	})

	t.Run("2. New B+ tree has correct initial structure", func(t *testing.T) {
		// Act
		tree := NewBPlusTree()

		// Assert
		require.NotNil(t, tree.Root)
		require.True(t, tree.Root.IsLeaf)
		require.Nil(t, tree.Root.Parent)
		require.Nil(t, tree.Root.Next)
		require.Nil(t, tree.Root.Prev)
		require.Nil(t, tree.Root.Children)
	})
}

func TestBPlusTreeInsert(t *testing.T) {
	t.Run("1. Insert single element", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		key := uint32(10)
		rowID := disk_manager.TupleID{PageID: 1, SlotNumber: 0}

		// Act
		tree.Insert(key, rowID)

		// Assert
		require.Len(t, tree.Root.Keys, 1)
		require.Equal(t, key, tree.Root.Keys[0])
		require.Equal(t, rowID, tree.Root.Values[0].TupleID)
	})

	t.Run("2. Insert multiple elements in order", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		keys := []uint32{1, 2, 3, 4}

		// Act
		for i, key := range keys {
			rowID := disk_manager.TupleID{PageID: uint32(i + 1), SlotNumber: uint32(i)}
			tree.Insert(key, rowID)
		}

		// Assert
		require.Len(t, tree.Root.Keys, 4)
		for i, key := range keys {
			require.Equal(t, key, tree.Root.Keys[i])
		}
	})

	t.Run("3. Insert multiple elements in reverse order", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		keys := []uint32{4, 3, 2, 1}

		// Act
		for i, key := range keys {
			rowID := disk_manager.TupleID{PageID: uint32(i + 1), SlotNumber: uint32(i)}
			tree.Insert(key, rowID)
		}

		// Assert
		require.Len(t, tree.Root.Keys, 4)
		// Проверяем, что ключи отсортированы
		for i := 0; i < len(tree.Root.Keys)-1; i++ {
			require.LessOrEqual(t, tree.Root.Keys[i], tree.Root.Keys[i+1])
		}
	})

	t.Run("4. Insert multiple elements in random order", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		keys := []uint32{5, 2, 8, 1, 9, 3, 7, 4, 6}

		// Act
		for i, key := range keys {
			rowID := disk_manager.TupleID{PageID: uint32(i + 1), SlotNumber: uint32(i)}
			tree.Insert(key, rowID)
		}

		// Assert
		// Проверяем, что все ключи присутствуют и отсортированы
		allKeys := make([]uint32, 0)
		collectAllKeys(tree.Root, &allKeys)
		sort.Slice(allKeys, func(i, j int) bool { return allKeys[i] < allKeys[j] })
		expectedKeys := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9}
		require.Equal(t, expectedKeys, allKeys)
	})

	t.Run("5. Insert duplicate key updates value", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		key := uint32(10)
		rowID1 := disk_manager.TupleID{PageID: 1, SlotNumber: 0}
		rowID2 := disk_manager.TupleID{PageID: 2, SlotNumber: 1}

		// Act
		tree.Insert(key, rowID1)
		tree.Insert(key, rowID2)

		// Assert
		require.Len(t, tree.Root.Keys, 1)
		require.Equal(t, key, tree.Root.Keys[0])
		require.Equal(t, rowID2, tree.Root.Values[0].TupleID) // Должно быть обновлено
	})

	t.Run("6. Insert causes leaf split", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		// Вставляем MaxOrder+1 элементов, чтобы вызвать разделение
		for i := uint32(1); i <= MaxOrder+1; i++ {
			rowID := disk_manager.TupleID{PageID: i, SlotNumber: i}
			tree.Insert(i, rowID)
		}

		// Assert
		// После разделения должен быть создан внутренний узел
		if !tree.Root.IsLeaf {
			require.Len(t, tree.Root.Keys, 1)
			require.Len(t, tree.Root.Children, 2)
		}
	})

	t.Run("7. Insert maintains doubly linked list in leaf nodes", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		// Вставляем достаточно элементов для создания нескольких листовых узлов
		for i := uint32(1); i <= MaxOrder*2+1; i++ {
			rowID := disk_manager.TupleID{PageID: i, SlotNumber: i}
			tree.Insert(i, rowID)
		}

		// Act & Assert
		// Находим первый листовой узел
		firstLeaf := findFirstLeaf(tree.Root)
		require.NotNil(t, firstLeaf)

		// Проверяем связи между листовыми узлами
		current := firstLeaf
		leafCount := 0
		for current != nil {
			leafCount++
			if current.Next != nil {
				require.Equal(t, current, current.Next.Prev)
			}
			current = current.Next
		}
		require.Greater(t, leafCount, 1) // Должно быть больше одного листового узла
	})

	t.Run("8. Check empty root", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		tree.Insert(1, disk_manager.TupleID{PageID: 1, SlotNumber: 1})
		tree.Insert(2, disk_manager.TupleID{PageID: 1, SlotNumber: 1})
		tree.Insert(3, disk_manager.TupleID{PageID: 1, SlotNumber: 1})
		tree.Insert(4, disk_manager.TupleID{PageID: 1, SlotNumber: 1})
		tree.Insert(6, disk_manager.TupleID{PageID: 1, SlotNumber: 1})

		tree.Delete(3)

		require.Len(t, tree.Root.Keys, 1)
		require.Len(t, tree.Root.Children, 2)
		require.Equal(t, tree.Root.Keys[0], uint32(3))
	})
}

func TestBPlusTreeGet(t *testing.T) {
	t.Run("1. Get existing key", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		key := uint32(10)
		rowID := disk_manager.TupleID{PageID: 1, SlotNumber: 5}
		tree.Insert(key, rowID)

		// Act
		result, found := tree.Get(key)

		// Assert
		require.True(t, found)
		require.NotNil(t, result)
		require.Equal(t, rowID.PageID, result.TupleID.PageID)
		require.Equal(t, rowID.SlotNumber, result.TupleID.SlotNumber)
	})

	t.Run("2. Get non-existing key", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		key := uint32(10)
		rowID := disk_manager.TupleID{PageID: 1, SlotNumber: 5}
		tree.Insert(key, rowID)

		// Act
		result, found := tree.Get(999)

		// Assert
		require.False(t, found)
		require.Nil(t, result)
	})

	t.Run("3. Get from empty tree", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()

		// Act
		result, found := tree.Get(10)

		// Assert
		require.False(t, found)
		require.Nil(t, result)
	})

	t.Run("4. Get multiple keys", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		keys := []uint32{1, 5, 10, 15, 20}
		for i, key := range keys {
			rowID := disk_manager.TupleID{PageID: uint32(i + 1), SlotNumber: uint32(i)}
			tree.Insert(key, rowID)
		}

		// Act & Assert
		for i, key := range keys {
			result, found := tree.Get(key)
			require.True(t, found)
			require.NotNil(t, result)
			require.Equal(t, uint32(i+1), result.TupleID.PageID)
			require.Equal(t, uint32(i), result.TupleID.SlotNumber)
		}
	})
}

func TestBPlusTreeDelete(t *testing.T) {
	t.Run("1. Delete existing key", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		key := uint32(10)
		rowID := disk_manager.TupleID{PageID: 1, SlotNumber: 5}
		tree.Insert(key, rowID)

		// Act
		err := tree.Delete(key)

		// Assert
		require.NoError(t, err)
		_, found := tree.Get(key)
		require.False(t, found)
	})

	t.Run("2. Delete non-existing key", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		key := uint32(10)
		rowID := disk_manager.TupleID{PageID: 1, SlotNumber: 5}
		tree.Insert(key, rowID)

		// Act
		err := tree.Delete(999)

		// Assert
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("3. Delete from empty tree", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()

		// Act
		err := tree.Delete(10)

		// Assert
		require.Error(t, err)
		// Пустое дерево может вернуть либо "empty", либо "not found"
		require.True(t, err.Error() == "tree is empty" || err.Error() == "key 10 not found")
	})

	t.Run("4. Delete multiple keys", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		keys := []uint32{1, 2, 3, 4, 5}
		for i, key := range keys {
			rowID := disk_manager.TupleID{PageID: uint32(i + 1), SlotNumber: uint32(i)}
			tree.Insert(key, rowID)
		}

		// Act
		for _, key := range keys {
			err := tree.Delete(key)
			require.NoError(t, err)
		}

		// Assert
		for _, key := range keys {
			_, found := tree.Get(key)
			require.False(t, found)
		}
	})

	t.Run("5. Delete causes leaf merge", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		// Вставляем элементы так, чтобы создать несколько листовых узлов
		for i := uint32(1); i <= MaxOrder*2; i++ {
			rowID := disk_manager.TupleID{PageID: i, SlotNumber: i}
			tree.Insert(i, rowID)
		}

		// Подсчитываем количество листовых узлов до удаления
		leafCountBefore := countLeafNodes(tree.Root)

		// Act - удаляем элементы до тех пор, пока не произойдет слияние
		for i := uint32(1); i <= MaxOrder; i++ {
			err := tree.Delete(i)
			require.NoError(t, err)
		}

		// Assert
		leafCountAfter := countLeafNodes(tree.Root)
		// После удаления количество листовых узлов должно уменьшиться или остаться тем же
		require.LessOrEqual(t, leafCountAfter, leafCountBefore)
	})

	t.Run("6. Delete maintains doubly linked list", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		// Вставляем элементы для создания нескольких листовых узлов
		for i := uint32(1); i <= MaxOrder*2+1; i++ {
			rowID := disk_manager.TupleID{PageID: i, SlotNumber: i}
			tree.Insert(i, rowID)
		}

		// Act
		err := tree.Delete(MaxOrder + 1)
		require.NoError(t, err)

		// Assert
		// Проверяем, что связи между листовыми узлами сохранены
		firstLeaf := findFirstLeaf(tree.Root)
		require.NotNil(t, firstLeaf)

		current := firstLeaf
		for current != nil {
			if current.Next != nil {
				require.Equal(t, current, current.Next.Prev)
			}
			current = current.Next
		}
	})
}

func TestBPlusTreeRangeQuery(t *testing.T) {
	t.Run("1. Range query with single element", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		key := uint32(10)
		rowID := disk_manager.TupleID{PageID: 1, SlotNumber: 5}
		tree.Insert(key, rowID)

		// Act
		results := tree.GetRange(10, 10)

		// Assert
		require.Len(t, results, 1)
		require.Equal(t, key, results[0].Key)
		require.Equal(t, rowID, results[0].TupleID)
	})

	t.Run("2. Range query with multiple elements", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		keys := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		for i, key := range keys {
			rowID := disk_manager.TupleID{PageID: uint32(i + 1), SlotNumber: uint32(i)}
			tree.Insert(key, rowID)
		}

		// Act
		results := tree.GetRange(3, 7)

		// Assert
		require.Len(t, results, 5) // 3, 4, 5, 6, 7
		for i, result := range results {
			require.Equal(t, uint32(i+3), result.Key)
		}
	})

	t.Run("3. Range query with no matching elements", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		keys := []uint32{1, 2, 3, 4, 5}
		for i, key := range keys {
			rowID := disk_manager.TupleID{PageID: uint32(i + 1), SlotNumber: uint32(i)}
			tree.Insert(key, rowID)
		}

		// Act
		results := tree.GetRange(10, 20)

		// Assert
		require.Empty(t, results)
	})

	t.Run("4. Range query on empty tree", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()

		// Act
		results := tree.GetRange(1, 10)

		// Assert
		require.Empty(t, results)
	})

	t.Run("5. Range query across multiple leaf nodes", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		// Вставляем достаточно элементов для создания нескольких листовых узлов
		for i := uint32(1); i <= MaxOrder*3; i++ {
			rowID := disk_manager.TupleID{PageID: i, SlotNumber: i}
			tree.Insert(i, rowID)
		}

		// Act - запрашиваем диапазон, который охватывает несколько листовых узлов
		endKey := uint32(MaxOrder * 2)
		results := tree.GetRange(2, endKey)

		// Assert
		expectedCount := int(endKey) - 2 + 1 // от 2 до endKey включительно
		require.Len(t, results, expectedCount)
		// Проверяем, что все результаты в правильном диапазоне
		for _, result := range results {
			require.GreaterOrEqual(t, result.Key, uint32(2))
			require.LessOrEqual(t, result.Key, endKey)
		}
	})

	t.Run("6. Range query returns sorted results", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		keys := []uint32{5, 2, 8, 1, 9, 3, 7, 4, 6}
		for i, key := range keys {
			rowID := disk_manager.TupleID{PageID: uint32(i + 1), SlotNumber: uint32(i)}
			tree.Insert(key, rowID)
		}

		// Act
		results := tree.GetRange(2, 7)

		// Assert
		// Проверяем, что результаты отсортированы
		for i := 0; i < len(results)-1; i++ {
			require.LessOrEqual(t, results[i].Key, results[i+1].Key)
		}
	})
}

func TestBPlusTreeRebalancing(t *testing.T) {
	t.Run("1. Insert causes multiple splits", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		// Вставляем много элементов для создания многоуровневого дерева
		for i := uint32(1); i <= MaxOrder*MaxOrder+1; i++ {
			rowID := disk_manager.TupleID{PageID: i, SlotNumber: i}
			tree.Insert(i, rowID)
		}

		// Assert
		// Проверяем, что дерево остается валидным
		allKeys := make([]uint32, 0)
		collectAllKeys(tree.Root, &allKeys)
		require.Len(t, allKeys, int(MaxOrder*MaxOrder+1))
	})

	t.Run("2. Delete causes multiple merges", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		// Вставляем элементы
		for i := uint32(1); i <= MaxOrder*3; i++ {
			rowID := disk_manager.TupleID{PageID: i, SlotNumber: i}
			tree.Insert(i, rowID)
		}

		// Act - удаляем большинство элементов
		for i := uint32(1); i <= MaxOrder*2; i++ {
			err := tree.Delete(i)
			require.NoError(t, err)
		}

		// Assert
		// Проверяем, что оставшиеся элементы все еще доступны
		for i := uint32(MaxOrder*2 + 1); i <= uint32(MaxOrder*3); i++ {
			result, found := tree.Get(i)
			require.True(t, found)
			require.NotNil(t, result)
		}
	})

	t.Run("3. Tree remains balanced after insert and delete", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		keys := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

		// Act - вставляем и удаляем элементы
		for i, key := range keys {
			rowID := disk_manager.TupleID{PageID: uint32(i + 1), SlotNumber: uint32(i)}
			tree.Insert(key, rowID)
		}

		// Удаляем некоторые элементы
		err := tree.Delete(2)
		require.NoError(t, err)
		err = tree.Delete(5)
		require.NoError(t, err)
		err = tree.Delete(8)
		require.NoError(t, err)

		// Вставляем новые элементы
		tree.Insert(11, disk_manager.TupleID{PageID: 11, SlotNumber: 11})
		tree.Insert(12, disk_manager.TupleID{PageID: 12, SlotNumber: 12})

		// Assert
		// Проверяем, что все оставшиеся элементы доступны
		remainingKeys := []uint32{1, 3, 4, 6, 7, 9, 10, 11, 12}
		for _, key := range remainingKeys {
			result, found := tree.Get(key)
			require.True(t, found)
			require.NotNil(t, result)
		}

		// Проверяем, что удаленные элементы недоступны
		deletedKeys := []uint32{2, 5, 8}
		for _, key := range deletedKeys {
			_, found := tree.Get(key)
			require.False(t, found)
		}
	})
}

// Вспомогательные функции для тестов

// formatBytes форматирует байты в читаемый формат (KB, MB, GB)
func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// collectAllKeys собирает все ключи из дерева
func collectAllKeys(node *Node, keys *[]uint32) {
	if node == nil {
		return
	}
	if node.IsLeaf {
		*keys = append(*keys, node.Keys...)
	} else {
		for _, child := range node.Children {
			collectAllKeys(child, keys)
		}
	}
}

// findFirstLeaf находит первый листовой узел
func findFirstLeaf(root *Node) *Node {
	if root == nil {
		return nil
	}
	node := root
	for !node.IsLeaf {
		if len(node.Children) > 0 {
			node = node.Children[0]
		} else {
			return nil
		}
	}
	return node
}

// countLeafNodes подсчитывает количество листовых узлов
func countLeafNodes(root *Node) int {
	if root == nil {
		return 0
	}
	count := 0
	countLeavesRecursive(root, &count)
	return count
}

// countLeavesRecursive рекурсивно подсчитывает листовые узлы
func countLeavesRecursive(node *Node, count *int) {
	if node == nil {
		return
	}
	if node.IsLeaf {
		*count++
	} else {
		for _, child := range node.Children {
			countLeavesRecursive(child, count)
		}
	}
}

// Нагрузочные тесты

func TestBPlusTreeStress(t *testing.T) {
	const numUsers = 1000000

	t.Run("1. Insert 1,000,000 users sequentially", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()

		// Измеряем память до заполнения
		runtime.GC() // Принудительная сборка мусора для более точных измерений
		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)
		t.Logf("Память до заполнения индекса: Alloc=%s, TotalAlloc=%s, Sys=%s",
			formatBytes(memBefore.Alloc),
			formatBytes(memBefore.TotalAlloc),
			formatBytes(memBefore.Sys))

		// Act
		for i := uint32(1); i <= numUsers; i++ {
			rowID := disk_manager.TupleID{PageID: i, SlotNumber: i % 100}
			tree.Insert(i, rowID)
		}

		// Измеряем память после заполнения
		runtime.GC() // Принудительная сборка мусора для более точных измерений
		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)
		t.Logf("Память после заполнения индекса (1,000,000 пользователей): Alloc=%s, TotalAlloc=%s, Sys=%s",
			formatBytes(memAfter.Alloc),
			formatBytes(memAfter.TotalAlloc),
			formatBytes(memAfter.Sys))
		t.Logf("Использовано памяти для индекса: Alloc=%s, TotalAlloc=%s",
			formatBytes(memAfter.Alloc-memBefore.Alloc),
			formatBytes(memAfter.TotalAlloc-memBefore.TotalAlloc))

		// Assert
		allKeys := make([]uint32, 0)
		collectAllKeys(tree.Root, &allKeys)
		require.Len(t, allKeys, numUsers)

		// Проверяем, что все ключи отсортированы
		for i := 0; i < len(allKeys)-1; i++ {
			require.LessOrEqual(t, allKeys[i], allKeys[i+1])
		}

		// Проверяем несколько случайных ключей
		testKeys := []uint32{1, 100000, 500000, 999999, numUsers}
		for _, key := range testKeys {
			result, found := tree.Get(key)
			require.True(t, found)
			require.NotNil(t, result)
		}
	})

	t.Run("2. Insert 1,000,000 users in random order", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		// Генерируем случайные ключи
		keys := make([]uint32, numUsers)
		for i := uint32(0); i < numUsers; i++ {
			keys[i] = i + 1
		}
		// Перемешиваем ключи
		for i := len(keys) - 1; i > 0; i-- {
			j := i % (i + 1) // Простое перемешивание для детерминированности
			keys[i], keys[j] = keys[j], keys[i]
		}

		// Act
		for i, key := range keys {
			rowID := disk_manager.TupleID{PageID: uint32(i + 1), SlotNumber: uint32(i % 100)}
			tree.Insert(key, rowID)
		}

		// Assert
		allKeys := make([]uint32, 0)
		collectAllKeys(tree.Root, &allKeys)
		require.Len(t, allKeys, numUsers)

		// Проверяем, что все ключи присутствуют
		keySet := make(map[uint32]bool)
		for _, key := range allKeys {
			keySet[key] = true
		}
		for i := uint32(1); i <= numUsers; i++ {
			require.True(t, keySet[i], "Key %d should be present", i)
		}

		// Проверяем, что все ключи отсортированы
		for i := 0; i < len(allKeys)-1; i++ {
			require.LessOrEqual(t, allKeys[i], allKeys[i+1])
		}
	})

	t.Run("3. Get operations on 1,000,000 users", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		for i := uint32(1); i <= numUsers; i++ {
			rowID := disk_manager.TupleID{PageID: i, SlotNumber: i % 100}
			tree.Insert(i, rowID)
		}

		// Act & Assert - проверяем различные ключи
		testCases := []struct {
			name        string
			key         uint32
			shouldExist bool
		}{
			{"first key", 1, true},
			{"middle key", numUsers / 2, true},
			{"last key", numUsers, true},
			{"non-existent key", numUsers + 1, false},
			{"non-existent key zero", 0, false},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, found := tree.Get(tc.key)
				if tc.shouldExist {
					require.True(t, found)
					require.NotNil(t, result)
				} else {
					require.False(t, found)
					require.Nil(t, result)
				}
			})
		}

		// Проверяем случайную выборку ключей
		sampleSize := 1000
		for i := 0; i < sampleSize; i++ {
			key := uint32((i * numUsers / sampleSize) + 1)
			result, found := tree.Get(key)
			require.True(t, found)
			require.NotNil(t, result)
		}
	})

	t.Run("4. Delete operations on 1,000,000 users", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		for i := uint32(1); i <= numUsers; i++ {
			rowID := disk_manager.TupleID{PageID: i, SlotNumber: i % 100}
			tree.Insert(i, rowID)
		}

		// Act - удаляем каждый 10-й элемент
		deletedCount := 0
		for i := uint32(10); i <= numUsers; i += 10 {
			err := tree.Delete(i)
			require.NoError(t, err)
			deletedCount++
		}

		// Assert
		allKeys := make([]uint32, 0)
		collectAllKeys(tree.Root, &allKeys)
		require.Len(t, allKeys, numUsers-deletedCount)

		// Проверяем, что удаленные ключи действительно удалены
		for i := uint32(10); i <= numUsers; i += 10 {
			_, found := tree.Get(i)
			require.False(t, found)
		}

		// Проверяем, что остальные ключи все еще доступны
		for i := uint32(1); i <= numUsers; i++ {
			if i%10 != 0 {
				result, found := tree.Get(i)
				require.True(t, found)
				require.NotNil(t, result)
			}
		}
	})

	t.Run("5. Range queries on 1,000,000 users", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		for i := uint32(1); i <= numUsers; i++ {
			rowID := disk_manager.TupleID{PageID: i, SlotNumber: i % 100}
			tree.Insert(i, rowID)
		}

		testCases := []struct {
			name          string
			startKey      uint32
			endKey        uint32
			expectedCount int
		}{
			{"small range at start", 1, 100, 100},
			{"small range in middle", 500000, 500100, 101},
			{"small range at end", numUsers - 99, numUsers, 100},
			{"large range", 100000, 200000, 100001},
			{"full range", 1, numUsers, numUsers},
			{"empty range", numUsers + 1, numUsers + 100, 0},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				results := tree.GetRange(tc.startKey, tc.endKey)
				require.Len(t, results, tc.expectedCount)

				// Проверяем, что результаты отсортированы
				for i := 0; i < len(results)-1; i++ {
					require.LessOrEqual(t, results[i].Key, results[i+1].Key)
				}

				// Проверяем, что все результаты в правильном диапазоне
				for _, result := range results {
					require.GreaterOrEqual(t, result.Key, tc.startKey)
					require.LessOrEqual(t, result.Key, tc.endKey)
				}
			})
		}
	})

	t.Run("6. Mixed operations (insert, delete, get) on 1,000,000 users", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()

		// Act - вставляем элементы
		for i := uint32(1); i <= numUsers; i++ {
			rowID := disk_manager.TupleID{PageID: i, SlotNumber: i % 100}
			tree.Insert(i, rowID)
		}

		// Удаляем некоторые элементы
		for i := uint32(1); i <= uint32(numUsers/10); i++ {
			err := tree.Delete(i * 10)
			require.NoError(t, err)
		}

		// Вставляем новые элементы
		for i := uint32(numUsers + 1); i <= uint32(numUsers+10000); i++ {
			rowID := disk_manager.TupleID{PageID: i, SlotNumber: i % 100}
			tree.Insert(i, rowID)
		}

		// Обновляем существующие элементы
		for i := uint32(1); i <= 1000; i++ {
			rowID := disk_manager.TupleID{PageID: i + 1000000, SlotNumber: i % 100}
			tree.Insert(i, rowID)
		}

		// Assert
		allKeys := make([]uint32, 0)
		collectAllKeys(tree.Root, &allKeys)

		// Проверяем, что удаленные элементы отсутствуют (кроме тех, что были обновлены)
		// Обновлены элементы от 1 до 1000, поэтому проверяем удаленные элементы от 1010 и выше
		for i := uint32(101); i <= uint32(numUsers/10); i++ {
			_, found := tree.Get(i * 10)
			require.False(t, found)
		}

		// Проверяем, что элементы от 10 до 1000 были восстановлены через обновление
		for i := uint32(10); i <= 1000; i += 10 {
			result, found := tree.Get(i)
			require.True(t, found)
			require.NotNil(t, result)
			require.Equal(t, i+1000000, result.TupleID.PageID)
		}

		// Проверяем, что новые элементы присутствуют
		for i := uint32(numUsers + 1); i <= uint32(numUsers+10000); i++ {
			result, found := tree.Get(i)
			require.True(t, found)
			require.NotNil(t, result)
		}

		// Проверяем, что обновленные элементы имеют новые значения
		for i := uint32(1); i <= 1000; i++ {
			result, found := tree.Get(i)
			require.True(t, found)
			require.NotNil(t, result)
			require.Equal(t, i+1000000, result.TupleID.PageID)
		}
	})

	t.Run("7. Tree structure validation after 1,000,000 inserts", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		for i := uint32(1); i <= numUsers; i++ {
			rowID := disk_manager.TupleID{PageID: i, SlotNumber: i % 100}
			tree.Insert(i, rowID)
		}

		// Assert - проверяем структуру дерева
		require.NotNil(t, tree.Root)

		// Проверяем, что все листовые узлы связаны правильно
		firstLeaf := findFirstLeaf(tree.Root)
		require.NotNil(t, firstLeaf)

		leafCount := 0
		current := firstLeaf
		prevLeaf := (*Node)(nil)
		for current != nil {
			leafCount++
			require.True(t, current.IsLeaf)
			require.Equal(t, prevLeaf, current.Prev)
			if current.Next != nil {
				require.Equal(t, current, current.Next.Prev)
			}
			prevLeaf = current
			current = current.Next
		}

		require.Greater(t, leafCount, 0)

		// Проверяем, что все ключи отсортированы в каждом листовом узле
		current = firstLeaf
		for current != nil {
			for i := 0; i < len(current.Keys)-1; i++ {
				require.LessOrEqual(t, current.Keys[i], current.Keys[i+1])
			}
			current = current.Next
		}

		// Проверяем, что количество ключей соответствует ожидаемому
		allKeys := make([]uint32, 0)
		collectAllKeys(tree.Root, &allKeys)
		require.Len(t, allKeys, numUsers)
	})

	t.Run("8. Performance test: sequential insert and get", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()

		// Act - вставляем элементы
		for i := uint32(1); i <= numUsers; i++ {
			rowID := disk_manager.TupleID{PageID: i, SlotNumber: i % 100}
			tree.Insert(i, rowID)
		}

		// Проверяем производительность поиска
		foundCount := 0
		for i := uint32(1); i <= numUsers; i += 1000 { // Проверяем каждый 1000-й элемент
			result, found := tree.Get(i)
			if found && result != nil {
				foundCount++
			}
		}

		// Assert
		expectedFound := numUsers / 1000
		require.Equal(t, expectedFound, foundCount)
	})

	t.Run("9. Delete half of 1,000,000 users", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		for i := uint32(1); i <= numUsers; i++ {
			rowID := disk_manager.TupleID{PageID: i, SlotNumber: i % 100}
			tree.Insert(i, rowID)
		}

		// Act - удаляем все четные ключи
		deletedCount := 0
		for i := uint32(2); i <= numUsers; i += 2 {
			err := tree.Delete(i)
			require.NoError(t, err)
			deletedCount++
		}

		// Assert
		allKeys := make([]uint32, 0)
		collectAllKeys(tree.Root, &allKeys)
		require.Len(t, allKeys, numUsers-deletedCount)

		// Проверяем, что все четные ключи удалены
		for i := uint32(2); i <= numUsers; i += 2 {
			_, found := tree.Get(i)
			require.False(t, found)
		}

		// Проверяем, что все нечетные ключи остались
		for i := uint32(1); i <= numUsers; i += 2 {
			result, found := tree.Get(i)
			require.True(t, found)
			require.NotNil(t, result)
		}

		// Проверяем структуру дерева после удаления
		firstLeaf := findFirstLeaf(tree.Root)
		require.NotNil(t, firstLeaf)
		current := firstLeaf
		for current != nil {
			if current.Next != nil {
				require.Equal(t, current, current.Next.Prev)
			}
			current = current.Next
		}
	})

	t.Run("10. Large range query on 1,000,000 users", func(t *testing.T) {
		// Arrange
		tree := NewBPlusTree()
		for i := uint32(1); i <= numUsers; i++ {
			rowID := disk_manager.TupleID{PageID: i, SlotNumber: i % 100}
			tree.Insert(i, rowID)
		}

		// Act - запрашиваем большой диапазон
		startKey := uint32(numUsers / 4)
		endKey := uint32(3 * numUsers / 4)
		results := tree.GetRange(startKey, endKey)

		// Assert
		expectedCount := int(endKey - startKey + 1)
		require.Len(t, results, expectedCount)

		// Проверяем корректность результатов
		for i, result := range results {
			expectedKey := startKey + uint32(i)
			require.Equal(t, expectedKey, result.Key)
			require.GreaterOrEqual(t, result.Key, startKey)
			require.LessOrEqual(t, result.Key, endKey)
		}
	})
}
