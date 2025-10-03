package buffer_bool

import (
	"custom-database/internal/disk_manager"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewBufferPool(t *testing.T) {
	t.Run("1. New buffer pool creation success", func(t *testing.T) {
		// Arrange
		maxSize := 10
		k := 2

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Очищаем перед тестом
		os.RemoveAll("tables")

		// Act
		bp, err := NewBufferPool(maxSize, k)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, bp)
		require.Implements(t, (*BufferPoolInterface)(nil), bp)

		// Приводим к конкретному типу для доступа к полям
		bufferPool := bp.(*BufferPool)
		require.NotNil(t, bufferPool.DiskManager)
	})

	t.Run("2. New buffer pool implements all interface methods", func(t *testing.T) {
		// Arrange
		maxSize := 5
		k := 2

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		bp, err := NewBufferPool(maxSize, k)
		require.NoError(t, err)

		// Assert - проверяем, что все методы интерфейса доступны
		require.NotNil(t, bp.GetPage)
		require.NotNil(t, bp.MarkDirty)
		require.NotNil(t, bp.Unpin)
		require.NotNil(t, bp.AddNewPage)
		require.NotNil(t, bp.ReadMetaInfo)
		require.NotNil(t, bp.WriteMetaInfo)
	})

	t.Run("3. Buffer pool with invalid parameters", func(t *testing.T) {
		// Arrange
		maxSize := 0
		k := 2

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Act
		bp, err := NewBufferPool(maxSize, k)

		// Assert
		require.NoError(t, err) // LRU-K может работать с 0 размером
		require.NotNil(t, bp)
	})
}

func TestBufferPoolGetPage(t *testing.T) {
	t.Run("1. Get page from empty buffer pool", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(5, 2)
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Очищаем перед тестом
		os.RemoveAll("tables")

		// Приводим к конкретному типу для доступа к полям
		bufferPool := bp.(*BufferPool)

		// Создаем тестовую таблицу
		tableName := "test_table"
		columns := []disk_manager.ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         disk_manager.INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		err = bufferPool.DiskManager.CreateDataBase()
		require.NoError(t, err)

		err = bp.CreateTable(tableName, columns)
		require.NoError(t, err)

		pageID := disk_manager.PageID{PageNumber: 1}

		// Сначала создаем страницу через AddNewPage
		_, err = bp.AddNewPage(tableName, pageID)
		require.NoError(t, err)

		// Act
		frame, err := bp.GetPage(tableName, pageID)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, frame)
		require.Equal(t, pageID, frame.PageID)
		require.Equal(t, tableName, frame.TableName)
		require.Equal(t, 2, frame.PinCount)
		require.True(t, frame.IsPinned)
		require.False(t, frame.IsDirty)
		require.NotNil(t, frame.Page)

		// Cleanup
		bp.DropTable(tableName)
	})

	t.Run("2. Get page from cache (hit)", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(5, 2)
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Очищаем перед тестом
		os.RemoveAll("tables")

		// Приводим к конкретному типу для доступа к полям
		bufferPool := bp.(*BufferPool)

		// Создаем тестовую таблицу
		tableName := "test_table"
		columns := []disk_manager.ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         disk_manager.INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		err = bufferPool.DiskManager.CreateDataBase()
		require.NoError(t, err)

		err = bp.CreateTable(tableName, columns)
		require.NoError(t, err)

		pageID := disk_manager.PageID{PageNumber: 1}

		// Сначала создаем страницу через AddNewPage
		_, err = bp.AddNewPage(tableName, pageID)
		require.NoError(t, err)

		// Первое чтение
		frame1, err := bp.GetPage(tableName, pageID)
		require.NoError(t, err)
		require.Equal(t, 2, frame1.PinCount)

		// Act - второе чтение той же страницы
		frame2, err := bp.GetPage(tableName, pageID)

		// Assert
		require.NoError(t, err)
		require.Equal(t, frame1, frame2)     // Тот же объект
		require.Equal(t, 3, frame2.PinCount) // PinCount увеличился (1 от AddNewPage + 2 от GetPage)

		// Cleanup
		bp.DropTable(tableName)
	})

	t.Run("3. Get page with eviction", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(2, 2) // Маленький буфер
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Очищаем перед тестом
		os.RemoveAll("tables")

		// Приводим к конкретному типу для доступа к полям
		bufferPool := bp.(*BufferPool)

		// Создаем тестовую таблицу
		tableName := "test_table"
		columns := []disk_manager.ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         disk_manager.INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		err = bufferPool.DiskManager.CreateDataBase()
		require.NoError(t, err)

		err = bp.CreateTable(tableName, columns)
		require.NoError(t, err)

		// Заполняем буфер
		pageID1 := disk_manager.PageID{PageNumber: 1}
		pageID2 := disk_manager.PageID{PageNumber: 2}

		// Создаем страницы
		_, err = bp.AddNewPage(tableName, pageID1)
		require.NoError(t, err)

		_, err = bp.AddNewPage(tableName, pageID2)
		require.NoError(t, err)

		_, err = bp.GetPage(tableName, pageID1)
		require.NoError(t, err)

		_, err = bp.GetPage(tableName, pageID2)
		require.NoError(t, err)

		// Освобождаем страницы для возможности вытеснения (убираем pin от GetPage)
		bp.Unpin(tableName, pageID1)
		bp.Unpin(tableName, pageID2)

		// Освобождаем страницы полностью (убираем pin от AddNewPage)
		bp.Unpin(tableName, pageID1)
		bp.Unpin(tableName, pageID2)

		// Проверяем, что страницы действительно разблокированы
		// Приводим к конкретному типу для доступа к полям
		bufferPool2 := bp.(*BufferPool)
		require.Equal(t, 0, bufferPool2.PinCounts[pageID1])
		require.Equal(t, 0, bufferPool2.PinCounts[pageID2])

		// Act - добавляем третью страницу (должна вытеснить одну из предыдущих)
		pageID3 := disk_manager.PageID{PageNumber: 3}
		_, err = bp.AddNewPage(tableName, pageID3)
		require.NoError(t, err)

		frame3, err := bp.GetPage(tableName, pageID3)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, frame3)
		require.Equal(t, pageID3, frame3.PageID)

		// Проверяем, что буфер содержит только 2 страницы
		require.Len(t, bufferPool.Pages, 2)

		// Cleanup
		bp.DropTable(tableName)
	})

	t.Run("4. Get non-existent page", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(5, 2)
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Очищаем перед тестом
		os.RemoveAll("tables")

		// Приводим к конкретному типу для доступа к полям
		bufferPool := bp.(*BufferPool)

		// Создаем тестовую таблицу
		tableName := "test_table"
		columns := []disk_manager.ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         disk_manager.INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		err = bufferPool.DiskManager.CreateDataBase()
		require.NoError(t, err)

		err = bp.CreateTable(tableName, columns)
		require.NoError(t, err)

		nonExistentPageID := disk_manager.PageID{PageNumber: 999}

		// Act - пытаемся прочитать несуществующую страницу
		frame, err := bp.GetPage(tableName, nonExistentPageID)

		// Assert
		require.Error(t, err)
		require.Nil(t, frame)
	})
}

func TestBufferPoolMarkDirty(t *testing.T) {
	t.Run("1. Mark page as dirty", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(5, 2)
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Очищаем перед тестом
		os.RemoveAll("tables")

		// Приводим к конкретному типу для доступа к полям
		bufferPool := bp.(*BufferPool)

		// Создаем тестовую таблицу
		tableName := "test_table"
		columns := []disk_manager.ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         disk_manager.INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		err = bufferPool.DiskManager.CreateDataBase()
		require.NoError(t, err)

		err = bp.CreateTable(tableName, columns)
		require.NoError(t, err)

		pageID := disk_manager.PageID{PageNumber: 1}

		// Сначала создаем страницу
		_, err = bp.AddNewPage(tableName, pageID)
		require.NoError(t, err)

		frame, err := bp.GetPage(tableName, pageID)
		require.NoError(t, err)
		require.False(t, frame.IsDirty)

		// Act
		bp.MarkDirty(tableName, pageID)

		// Assert
		require.True(t, frame.IsDirty)
		require.True(t, bufferPool.DirtyPages[pageID])

		// Cleanup
		bp.DropTable(tableName)
	})

	t.Run("2. Mark non-existent page as dirty", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(5, 2)
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		nonExistentPageID := disk_manager.PageID{PageNumber: 999}

		// Act
		bp.MarkDirty("non_existent_table", nonExistentPageID)

		// Assert - не должно паниковать, но и не должно ничего делать
		// Приводим к конкретному типу для доступа к полям
		bufferPool := bp.(*BufferPool)
		require.False(t, bufferPool.DirtyPages[nonExistentPageID])
	})
}

func TestBufferPoolUnpin(t *testing.T) {
	t.Run("1. Unpin page", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(5, 2)
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Очищаем перед тестом
		os.RemoveAll("tables")

		// Приводим к конкретному типу для доступа к полям
		bufferPool := bp.(*BufferPool)

		// Создаем тестовую таблицу
		tableName := "test_table"
		columns := []disk_manager.ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         disk_manager.INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		err = bufferPool.DiskManager.CreateDataBase()
		require.NoError(t, err)

		err = bp.CreateTable(tableName, columns)
		require.NoError(t, err)

		pageID := disk_manager.PageID{PageNumber: 1}

		// Сначала создаем страницу
		_, err = bp.AddNewPage(tableName, pageID)
		require.NoError(t, err)

		frame, err := bp.GetPage(tableName, pageID)
		require.NoError(t, err)
		require.Equal(t, 2, frame.PinCount)
		require.True(t, frame.IsPinned)

		// Act
		bp.Unpin(tableName, pageID)

		// Assert
		require.Equal(t, 1, frame.PinCount)
		require.True(t, frame.IsPinned)
		require.Equal(t, 1, bufferPool.PinCounts[pageID])

		// Cleanup
		bp.DropTable(tableName)
	})

	t.Run("2. Unpin page multiple times", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(5, 2)
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Очищаем перед тестом
		os.RemoveAll("tables")

		// Приводим к конкретному типу для доступа к полям
		bufferPool := bp.(*BufferPool)

		// Создаем тестовую таблицу
		tableName := "test_table"
		columns := []disk_manager.ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         disk_manager.INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		err = bufferPool.DiskManager.CreateDataBase()
		require.NoError(t, err)

		err = bp.CreateTable(tableName, columns)
		require.NoError(t, err)

		pageID := disk_manager.PageID{PageNumber: 1}

		// Сначала создаем страницу
		_, err = bp.AddNewPage(tableName, pageID)
		require.NoError(t, err)

		// Получаем страницу дважды (PinCount = 2)
		frame1, err := bp.GetPage(tableName, pageID)
		require.NoError(t, err)

		frame2, err := bp.GetPage(tableName, pageID)
		require.NoError(t, err)
		require.Equal(t, frame1, frame2)
		require.Equal(t, 3, frame2.PinCount)

		// Cleanup
		bp.DropTable(tableName)

		// Act - unpin дважды
		bp.Unpin(tableName, pageID)
		require.Equal(t, 2, frame2.PinCount)
		require.True(t, frame2.IsPinned)

		bp.Unpin(tableName, pageID)

		// Assert
		require.Equal(t, 1, frame2.PinCount)
		require.True(t, frame2.IsPinned)
		require.Equal(t, 1, bufferPool.PinCounts[pageID])

		// Cleanup
		bp.DropTable(tableName)
	})

	t.Run("3. Unpin non-existent page", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(5, 2)
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		nonExistentPageID := disk_manager.PageID{PageNumber: 999}

		// Act
		bp.Unpin("non_existent_table", nonExistentPageID)

		// Assert - не должно паниковать
		// Приводим к конкретному типу для доступа к полям
		bufferPool := bp.(*BufferPool)
		require.Equal(t, 0, bufferPool.PinCounts[nonExistentPageID])
	})
}

func TestBufferPoolAddNewPage(t *testing.T) {
	t.Run("1. Add new page", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(5, 2)
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Очищаем перед тестом
		os.RemoveAll("tables")

		// Приводим к конкретному типу для доступа к полям
		bufferPool := bp.(*BufferPool)

		// Создаем тестовую таблицу
		tableName := "test_table"
		columns := []disk_manager.ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         disk_manager.INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		err = bufferPool.DiskManager.CreateDataBase()
		require.NoError(t, err)

		err = bp.CreateTable(tableName, columns)
		require.NoError(t, err)

		pageID := disk_manager.PageID{PageNumber: 1}

		// Act
		frame, err := bp.AddNewPage(tableName, pageID)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, frame)
		require.Equal(t, pageID, frame.PageID)
		require.Equal(t, tableName, frame.TableName)
		require.Equal(t, 1, frame.PinCount)
		require.True(t, frame.IsPinned)
		require.False(t, frame.IsDirty)
		require.NotNil(t, frame.Page)
		require.Contains(t, bufferPool.Pages, pageID)

		// Cleanup
		bp.DropTable(tableName)
	})

	t.Run("2. Add new page with eviction", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(2, 2) // Маленький буфер
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Очищаем перед тестом
		os.RemoveAll("tables")

		// Приводим к конкретному типу для доступа к полям
		bufferPool := bp.(*BufferPool)

		// Создаем тестовую таблицу
		tableName := "test_table"
		columns := []disk_manager.ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         disk_manager.INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		err = bufferPool.DiskManager.CreateDataBase()
		require.NoError(t, err)

		err = bp.CreateTable(tableName, columns)
		require.NoError(t, err)

		// Заполняем буфер
		pageID1 := disk_manager.PageID{PageNumber: 1}
		pageID2 := disk_manager.PageID{PageNumber: 2}

		// Создаем страницы
		_, err = bp.AddNewPage(tableName, pageID1)
		require.NoError(t, err)

		_, err = bp.AddNewPage(tableName, pageID2)
		require.NoError(t, err)

		_, err = bp.GetPage(tableName, pageID1)
		require.NoError(t, err)

		_, err = bp.GetPage(tableName, pageID2)
		require.NoError(t, err)

		// Освобождаем страницы для возможности вытеснения (убираем pin от GetPage)
		bp.Unpin(tableName, pageID1)
		bp.Unpin(tableName, pageID2)

		// Освобождаем страницы полностью (убираем pin от AddNewPage)
		bp.Unpin(tableName, pageID1)
		bp.Unpin(tableName, pageID2)

		// Act - добавляем новую страницу (должна вытеснить одну из предыдущих)
		pageID3 := disk_manager.PageID{PageNumber: 3}
		frame3, err := bp.AddNewPage(tableName, pageID3)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, frame3)
		require.Equal(t, pageID3, frame3.PageID)

		// Проверяем, что буфер содержит только 2 страницы
		require.Len(t, bufferPool.Pages, 2)

		// Cleanup
		bp.DropTable(tableName)
	})

	t.Run("3. Add new page to non-existent table", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(5, 2)
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		pageID := disk_manager.PageID{PageNumber: 1}

		// Act
		frame, err := bp.AddNewPage("non_existent_table", pageID)

		// Assert
		require.Error(t, err)
		require.Nil(t, frame)
	})
}

func TestBufferPoolReadMetaInfo(t *testing.T) {
	t.Run("1. Read meta info for existing table", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(5, 2)
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Очищаем перед тестом
		os.RemoveAll("tables")

		// Приводим к конкретному типу для доступа к полям
		bufferPool := bp.(*BufferPool)

		// Создаем тестовую таблицу
		tableName := "test_table"
		columns := []disk_manager.ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         disk_manager.INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		err = bufferPool.DiskManager.CreateDataBase()
		require.NoError(t, err)

		err = bp.CreateTable(tableName, columns)
		require.NoError(t, err)

		// Act
		metaInfo, err := bp.ReadMetaInfo(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, metaInfo)
		require.NotNil(t, metaInfo.MetaData)
		require.NotNil(t, metaInfo.PageDirectory)
		require.NotNil(t, metaInfo.DataHeaders)

		// Cleanup
		bp.DropTable(tableName)
	})

	t.Run("2. Read meta info for non-existent table", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(5, 2)
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Act
		metaInfo, err := bp.ReadMetaInfo("non_existent_table")

		// Assert
		require.NoError(t, err) // Возвращает nil, но не ошибку
		require.Nil(t, metaInfo)
	})
}

func TestBufferPoolWriteMetaInfo(t *testing.T) {
	t.Run("1. Write meta info", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(5, 2)
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Очищаем перед тестом
		os.RemoveAll("tables")

		// Приводим к конкретному типу для доступа к полям
		bufferPool := bp.(*BufferPool)

		// Создаем тестовую таблицу
		tableName := "test_table"
		columns := []disk_manager.ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         disk_manager.INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		err = bufferPool.DiskManager.CreateDataBase()
		require.NoError(t, err)

		err = bp.CreateTable(tableName, columns)
		require.NoError(t, err)

		// Читаем метаинформацию
		metaInfo, err := bp.ReadMetaInfo(tableName)
		require.NoError(t, err)
		require.NotNil(t, metaInfo)

		// Изменяем метаинформацию
		metaInfo.MetaData.Header.NextRowID = 5

		// Act
		err = bp.WriteMetaInfo(tableName)

		// Assert
		require.NoError(t, err)

		// Проверяем, что изменения сохранились
		updatedMetaInfo, err := bp.ReadMetaInfo(tableName)
		require.NoError(t, err)
		require.Equal(t, uint64(5), updatedMetaInfo.MetaData.Header.NextRowID)

		// Cleanup
		bp.DropTable(tableName)
	})

	t.Run("2. Write meta info for non-existent table", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(5, 2)
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Act
		err = bp.WriteMetaInfo("non_existent_table")

		// Assert
		require.Error(t, err) // Должна быть ошибка, так как метаинформация не найдена
	})
}

func TestBufferPoolComplexScenario(t *testing.T) {
	t.Run("1. Complex scenario with multiple operations", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(2, 2) // Маленький буфер для тестирования вытеснения
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Очищаем перед тестом
		os.RemoveAll("tables")

		// Приводим к конкретному типу для доступа к полям
		bufferPool := bp.(*BufferPool)

		// Создаем тестовую таблицу
		tableName := "test_table"
		columns := []disk_manager.ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         disk_manager.INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		err = bufferPool.DiskManager.CreateDataBase()
		require.NoError(t, err)

		err = bp.CreateTable(tableName, columns)
		require.NoError(t, err)

		// Act & Assert

		// 1. Создаем и читаем страницу 1
		pageID0 := disk_manager.PageID{PageNumber: 1}
		_, err = bp.AddNewPage(tableName, pageID0)
		require.NoError(t, err)

		frame0, err := bp.GetPage(tableName, pageID0)
		require.NoError(t, err)
		require.Equal(t, 2, frame0.PinCount)
		require.Len(t, bufferPool.Pages, 1)

		// 2. Создаем и читаем страницу 2
		pageID1 := disk_manager.PageID{PageNumber: 2}
		_, err = bp.AddNewPage(tableName, pageID1)
		require.NoError(t, err)

		frame1, err := bp.GetPage(tableName, pageID1)
		require.NoError(t, err)
		require.Equal(t, 2, frame1.PinCount)
		require.Len(t, bufferPool.Pages, 2)

		// 3. Освобождаем страницы для возможности вытеснения (убираем pin от GetPage)
		bp.Unpin(tableName, pageID0)
		bp.Unpin(tableName, pageID1)

		// 3.1. Освобождаем страницы полностью (убираем pin от AddNewPage)
		bp.Unpin(tableName, pageID0)
		bp.Unpin(tableName, pageID1)

		// 4. Создаем и читаем страницу 3 (должна вытеснить одну из предыдущих)
		pageID2 := disk_manager.PageID{PageNumber: 3}
		_, err = bp.AddNewPage(tableName, pageID2)
		require.NoError(t, err)

		frame2, err := bp.GetPage(tableName, pageID2)
		require.NoError(t, err)
		require.Equal(t, 2, frame2.PinCount)
		require.Len(t, bufferPool.Pages, 2)

		// 5. Помечаем страницу 1 как dirty
		bp.MarkDirty(tableName, pageID1)
		require.True(t, frame1.IsDirty)
		// Приводим к конкретному типу для доступа к полям
		bufferPool2 := bp.(*BufferPool)
		require.True(t, bufferPool2.DirtyPages[pageID1])

		// 6. Страница 0 уже была вытеснена на шаге 4, поэтому не нужно её освобождать

		// 6.1. Освобождаем страницу 3 для возможности вытеснения
		bp.Unpin(tableName, pageID2) // Убираем pin от GetPage
		bp.Unpin(tableName, pageID2) // Убираем pin от AddNewPage

		// 7. Добавляем новую страницу (должна вытеснить страницу 2)
		pageID3 := disk_manager.PageID{PageNumber: 4}
		frame3, err := bp.AddNewPage(tableName, pageID3)
		require.NoError(t, err)
		require.Equal(t, 1, frame3.PinCount)
		require.Len(t, bufferPool.Pages, 2)

		// 8. Проверяем, что в буфере остались страницы 3 и 4
		require.Contains(t, bufferPool.Pages, pageID2)
		require.Contains(t, bufferPool.Pages, pageID3)

		// 9. Читаем метаинформацию
		metaInfo, err := bp.ReadMetaInfo(tableName)
		require.NoError(t, err)
		require.NotNil(t, metaInfo)

		// 10. Изменяем и записываем метаинформацию
		metaInfo.MetaData.Header.NextRowID = 10
		err = bp.WriteMetaInfo(tableName)
		require.NoError(t, err)

		// 11. Проверяем, что изменения сохранились
		updatedMetaInfo, err := bp.ReadMetaInfo(tableName)
		require.NoError(t, err)
		require.Equal(t, uint64(10), updatedMetaInfo.MetaData.Header.NextRowID)

		// Cleanup
		bp.DropTable(tableName)
	})
}

func TestBufferPoolBackgroundWorker(t *testing.T) {
	t.Run("1. Background worker flushes dirty pages", func(t *testing.T) {
		// Arrange
		bp, err := NewBufferPool(5, 2)
		require.NoError(t, err)

		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Очищаем перед тестом
		os.RemoveAll("tables")

		// Приводим к конкретному типу для доступа к полям
		bufferPool := bp.(*BufferPool)

		// Создаем тестовую таблицу
		tableName := "test_table"
		columns := []disk_manager.ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         disk_manager.INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		err = bufferPool.DiskManager.CreateDataBase()
		require.NoError(t, err)

		err = bp.CreateTable(tableName, columns)
		require.NoError(t, err)

		// Создаем страницу и помечаем как dirty
		pageID := disk_manager.PageID{PageNumber: 1}
		_, err = bp.AddNewPage(tableName, pageID)
		require.NoError(t, err)

		frame, err := bp.GetPage(tableName, pageID)
		require.NoError(t, err)

		bp.MarkDirty(tableName, pageID)
		require.True(t, frame.IsDirty)
		require.True(t, bufferPool.DirtyPages[pageID])

		// Act - ждем, пока background worker сработает
		time.Sleep(4 * time.Second)

		// Assert - проверяем, что страница больше не dirty
		// (background worker должен был записать её на диск)
		require.False(t, frame.IsDirty)
		// Приводим к конкретному типу для доступа к полям
		bufferPool2 := bp.(*BufferPool)
		require.False(t, bufferPool2.DirtyPages[pageID])

		// Cleanup
		bp.DropTable(tableName)
	})
}
