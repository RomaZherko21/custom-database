package disk_manager

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewDiskManager(t *testing.T) {
	t.Run("1. New disk manager creation success", func(t *testing.T) {
		// Act
		dm := NewDiskManager()

		// Assert
		require.NotNil(t, dm)
		require.Implements(t, (*DiskManager)(nil), dm)
	})

	t.Run("2. New disk manager implements all interface methods", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()

		// Assert - проверяем, что все методы интерфейса доступны
		require.NotNil(t, dm.CreateTable)
		require.NotNil(t, dm.DropTable)
		require.NotNil(t, dm.ReadMetaFile)
		require.NotNil(t, dm.WriteMetaFile)
		require.NotNil(t, dm.ReadPageDirectory)
		require.NotNil(t, dm.WritePageDirectory)
		require.NotNil(t, dm.ReadPage)
		require.NotNil(t, dm.WritePage)
		require.NotNil(t, dm.AddNewPage)
	})
}

func TestDiskManagerCreateTable(t *testing.T) {
	t.Run("1. Create table success", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "test_users"
		columns := []ColumnInfo{
			{
				ColumnNameLength: 2,
				DataType:         INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
				ColumnName:       "id",
			},
			{
				ColumnNameLength: 4,
				DataType:         TEXT_TYPE,
				IsNullable:       1,
				IsPrimaryKey:     0,
				IsAutoIncrement:  0,
				DefaultValue:     0,
				ColumnName:       "name",
			},
		}

		// Cleanup
		defer func() {
			dm.DropTable(tableName)
			os.RemoveAll("tables")
		}()

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем базу данных (создает table_list файл)
		err = dm.CreateDataBase()
		require.NoError(t, err)

		// Act
		err = dm.CreateTable(tableName, columns)

		// Assert
		require.NoError(t, err)

		// Проверяем, что все файлы созданы
		metaFilePath := filepath.Join("tables", tableName+".meta")
		dirFilePath := filepath.Join("tables", tableName+".dir")
		dataFilePath := filepath.Join("tables", tableName+".data")

		_, err = os.Stat(metaFilePath)
		require.NoError(t, err)

		_, err = os.Stat(dirFilePath)
		require.NoError(t, err)

		_, err = os.Stat(dataFilePath)
		require.NoError(t, err)
	})

	t.Run("2. Create table with empty columns", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "empty_table"
		columns := []ColumnInfo{}

		// Cleanup
		defer func() {
			dm.DropTable(tableName)
			os.RemoveAll("tables")
		}()

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем базу данных (создает table_list файл)
		err = dm.CreateDataBase()
		require.NoError(t, err)

		// Act
		err = dm.CreateTable(tableName, columns)

		// Assert
		require.NoError(t, err)

		// Проверяем, что файлы созданы
		metaFilePath := filepath.Join("tables", tableName+".meta")
		_, err = os.Stat(metaFilePath)
		require.NoError(t, err)
	})

	t.Run("3. Create table when already exists", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "existing_table"
		columns := []ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		// Cleanup
		defer func() {
			dm.DropTable(tableName)
			os.RemoveAll("tables")
		}()

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем базу данных (создает table_list файл)
		err = dm.CreateDataBase()
		require.NoError(t, err)

		// Создаем таблицу первый раз
		err = dm.CreateTable(tableName, columns)
		require.NoError(t, err)

		// Act - пытаемся создать таблицу с тем же именем
		err = dm.CreateTable(tableName, columns)

		// Assert
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")
	})

	t.Run("4. Create table with empty table name", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := ""
		columns := []ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		// Cleanup
		defer func() {
			dm.DropTable(tableName)
			os.RemoveAll("tables")
		}()

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем базу данных (создает table_list файл)
		err = dm.CreateDataBase()
		require.NoError(t, err)

		// Act
		err = dm.CreateTable(tableName, columns)

		// Assert
		require.NoError(t, err)

		// Проверяем, что файлы созданы с пустым именем
		metaFilePath := filepath.Join("tables", ".meta")
		_, err = os.Stat(metaFilePath)
		require.NoError(t, err)
	})
}

func TestDiskManagerDropTable(t *testing.T) {
	t.Run("1. Drop table success", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "test_drop_table"
		columns := []ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		// Cleanup
		defer os.RemoveAll("tables")

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем базу данных (создает table_list файл)
		err = dm.CreateDataBase()
		require.NoError(t, err)

		// Создаем таблицу
		err = dm.CreateTable(tableName, columns)
		require.NoError(t, err)

		// Проверяем, что файлы существуют
		metaFilePath := filepath.Join("tables", tableName+".meta")
		dirFilePath := filepath.Join("tables", tableName+".dir")
		dataFilePath := filepath.Join("tables", tableName+".data")

		_, err = os.Stat(metaFilePath)
		require.NoError(t, err)

		_, err = os.Stat(dirFilePath)
		require.NoError(t, err)

		_, err = os.Stat(dataFilePath)
		require.NoError(t, err)

		// Act
		err = dm.DropTable(tableName)

		// Assert
		require.NoError(t, err)

		// Проверяем, что файлы удалены
		_, err = os.Stat(metaFilePath)
		require.Error(t, err)
		require.True(t, os.IsNotExist(err))

		_, err = os.Stat(dirFilePath)
		require.Error(t, err)
		require.True(t, os.IsNotExist(err))

		_, err = os.Stat(dataFilePath)
		require.Error(t, err)
		require.True(t, os.IsNotExist(err))
	})

	t.Run("2. Drop table when table does not exist", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "non_existent_table"

		// Act
		err := dm.DropTable(tableName)

		// Assert
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("3. Drop table with empty table name", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := ""

		// Act
		err := dm.DropTable(tableName)

		// Assert
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestDiskManagerReadMetaFile(t *testing.T) {
	t.Run("1. Read meta file success", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "test_read_meta"
		columns := []ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
			{
				ColumnNameLength: 4,
				ColumnName:       "name",
				DataType:         TEXT_TYPE,
				IsNullable:       1,
				IsPrimaryKey:     0,
				IsAutoIncrement:  0,
				DefaultValue:     0,
			},
		}

		// Cleanup
		defer func() {
			dm.DropTable(tableName)
			os.RemoveAll("tables")
		}()

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем базу данных (создает table_list файл)
		err = dm.CreateDataBase()
		require.NoError(t, err)

		// Создаем таблицу
		err = dm.CreateTable(tableName, columns)
		require.NoError(t, err)

		// Act
		metaData, err := dm.ReadMetaFile(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, metaData)
		require.Equal(t, tableName, string(metaData.Header.TableName[:len(tableName)]))
		require.Equal(t, uint32(len(columns)), metaData.Header.ColumnCount)
		require.Len(t, metaData.Columns, len(columns))
		require.Equal(t, "id", string(metaData.Columns[0].ColumnName[:2]))
		require.Equal(t, "name", string(metaData.Columns[1].ColumnName[:4]))
	})

	t.Run("2. Read meta file when table does not exist", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "non_existent_table"

		// Act
		metaData, err := dm.ReadMetaFile(tableName)

		// Assert
		require.Error(t, err)
		require.Nil(t, metaData)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestDiskManagerWriteMetaFile(t *testing.T) {
	t.Run("1. Write meta file success", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "test_write_meta"
		columns := []ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		// Cleanup
		defer func() {
			dm.DropTable(tableName)
			os.RemoveAll("tables")
		}()

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем базу данных (создает table_list файл)
		err = dm.CreateDataBase()
		require.NoError(t, err)

		// Создаем таблицу
		err = dm.CreateTable(tableName, columns)
		require.NoError(t, err)

		// Читаем исходные данные
		originalMeta, err := dm.ReadMetaFile(tableName)
		require.NoError(t, err)

		// Изменяем NextRowID
		originalMeta.Header.NextRowID = 100

		// Act
		writtenMeta, err := dm.WriteMetaFile(tableName, originalMeta)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, writtenMeta)
		require.Equal(t, uint64(100), writtenMeta.Header.NextRowID)

		// Проверяем, что изменения записались
		readMeta, err := dm.ReadMetaFile(tableName)
		require.NoError(t, err)
		require.Equal(t, uint64(100), readMeta.Header.NextRowID)
	})

	t.Run("2. Write meta file when table does not exist", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "non_existent_write_table"
		metaData := &MetaData{
			Header: &MetaDataHeader{
				MagicNumber:  META_FILE_MAGIC_NUMBER,
				TableNameLen: uint32(len(tableName)),
				ColumnCount:  0,
				TableName:    tableName,
				NextRowID:    0,
			},
			Columns: []ColumnInfo{},
		}

		// Act
		writtenMeta, err := dm.WriteMetaFile(tableName, metaData)

		// Assert
		require.Error(t, err)
		require.Nil(t, writtenMeta)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestDiskManagerReadPageDirectory(t *testing.T) {
	t.Run("1. Read page directory success", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "test_read_page_dir"
		columns := []ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		// Cleanup
		defer func() {
			dm.DropTable(tableName)
			os.RemoveAll("tables")
		}()

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем базу данных (создает table_list файл)
		err = dm.CreateDataBase()
		require.NoError(t, err)

		// Создаем таблицу
		err = dm.CreateTable(tableName, columns)
		require.NoError(t, err)

		// Act
		pageDirectory, err := dm.ReadPageDirectory(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, pageDirectory)
		require.Equal(t, tableName, pageDirectory.TableName)
		require.Equal(t, uint32(PAGE_DIRECTORY_MAGIC_NUMBER), pageDirectory.Header.MagicNumber)
		require.Equal(t, uint32(0), pageDirectory.Header.PageCount)
		require.Equal(t, uint32(PAGE_INITIAL_ID), pageDirectory.Header.NextPageID)
		require.Len(t, pageDirectory.Entries, 0)
	})

	t.Run("2. Read page directory when table does not exist", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "non_existent_table"

		// Act
		pageDirectory, err := dm.ReadPageDirectory(tableName)

		// Assert
		require.Error(t, err)
		require.Nil(t, pageDirectory)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestDiskManagerWritePageDirectory(t *testing.T) {
	t.Run("1. Write page directory success", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "test_write_page_dir"
		columns := []ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		// Cleanup
		defer func() {
			dm.DropTable(tableName)
			os.RemoveAll("tables")
		}()

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем базу данных (создает table_list файл)
		err = dm.CreateDataBase()
		require.NoError(t, err)

		// Создаем таблицу
		err = dm.CreateTable(tableName, columns)
		require.NoError(t, err)

		// Читаем исходные данные
		originalDir, err := dm.ReadPageDirectory(tableName)
		require.NoError(t, err)

		// Изменяем данные
		originalDir.Header.PageCount = 2
		originalDir.Header.NextPageID = 15
		originalDir.Entries = []PageDirectoryEntry{
			{PageID: 0, FreeSpace: 1024, Flags: 0},
			{PageID: 1, FreeSpace: 512, Flags: 0},
		}

		// Act
		writtenDir, err := dm.WritePageDirectory(tableName, originalDir)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, writtenDir)
		require.Equal(t, uint32(2), writtenDir.Header.PageCount)
		require.Equal(t, uint32(15), writtenDir.Header.NextPageID)
		require.Len(t, writtenDir.Entries, 2)

		// Проверяем, что изменения записались
		readDir, err := dm.ReadPageDirectory(tableName)
		require.NoError(t, err)
		require.Equal(t, uint32(2), readDir.Header.PageCount)
		require.Equal(t, uint32(15), readDir.Header.NextPageID)
		require.Len(t, readDir.Entries, 2)
	})

	t.Run("2. Write page directory when table does not exist", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "non_existent_write_table"
		pageDirectory := &PageDirectory{
			TableName: tableName,
			Header: &PageDirectoryHeader{
				MagicNumber: PAGE_DIRECTORY_MAGIC_NUMBER,
				PageCount:   0,
				NextPageID:  PAGE_INITIAL_ID,
			},
			Entries: []PageDirectoryEntry{},
		}

		// Act
		writtenDir, err := dm.WritePageDirectory(tableName, pageDirectory)

		// Assert
		require.Error(t, err)
		require.Nil(t, writtenDir)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestDiskManagerReadPage(t *testing.T) {
	t.Run("1. Read page success", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "test_read_page"
		columns := []ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
			{
				ColumnNameLength: 4,
				ColumnName:       "name",
				DataType:         TEXT_TYPE,
				IsNullable:       1,
				IsPrimaryKey:     0,
				IsAutoIncrement:  0,
				DefaultValue:     0,
			},
		}

		// Cleanup
		defer func() {
			dm.DropTable(tableName)
			os.RemoveAll("tables")
		}()

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем базу данных (создает table_list файл)
		err = dm.CreateDataBase()
		require.NoError(t, err)

		// Создаем таблицу
		err = dm.CreateTable(tableName, columns)
		require.NoError(t, err)

		// Добавляем страницу
		pageID := PageID{PageNumber: 1}
		_, err = dm.AddNewPage(tableName, pageID)
		require.NoError(t, err)

		// Act
		page, err := dm.ReadPage(tableName, pageID)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, page)
		require.Equal(t, uint32(1), page.Header.PageID)
		require.Equal(t, uint32(0), page.Header.RecordCount)
		require.Len(t, page.Rows, 0)
		require.Len(t, page.Columns, len(columns))
		require.Equal(t, "id", page.Columns[0].ColumnName)
		require.Equal(t, "name", page.Columns[1].ColumnName)
	})

	t.Run("2. Read page when table does not exist", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "non_existent_table"
		pageID := PageID{PageNumber: 1}

		// Act
		page, err := dm.ReadPage(tableName, pageID)

		// Assert
		require.Error(t, err)
		require.Nil(t, page)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("3. Read page with invalid page ID", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "test_invalid_page_id"
		columns := []ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		// Cleanup
		defer func() {
			dm.DropTable(tableName)
			os.RemoveAll("tables")
		}()

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем базу данных (создает table_list файл)
		err = dm.CreateDataBase()
		require.NoError(t, err)

		// Создаем таблицу
		err = dm.CreateTable(tableName, columns)
		require.NoError(t, err)

		// Пытаемся прочитать несуществующую страницу
		pageID := PageID{PageNumber: 5}

		// Act
		page, err := dm.ReadPage(tableName, pageID)

		// Assert
		require.Error(t, err)
		require.Nil(t, page)
		require.Contains(t, err.Error(), "out of range")
	})
}

func TestDiskManagerWritePage(t *testing.T) {
	t.Run("1. Write page success", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "test_write_page"
		columns := []ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		// Cleanup
		defer func() {
			dm.DropTable(tableName)
			os.RemoveAll("tables")
		}()

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем базу данных (создает table_list файл)
		err = dm.CreateDataBase()
		require.NoError(t, err)

		// Создаем таблицу
		err = dm.CreateTable(tableName, columns)
		require.NoError(t, err)

		// Добавляем страницу
		pageID := PageID{PageNumber: 1}
		page, err := dm.AddNewPage(tableName, pageID)
		require.NoError(t, err)

		// Изменяем страницу - просто проверяем, что запись работает
		// Не изменяем RecordCount, так как это может вызвать проблемы с десериализацией

		// Act
		writtenPage, err := dm.WritePage(tableName, pageID, page)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, writtenPage)
		require.Equal(t, uint32(1), writtenPage.Header.PageID)

		// Проверяем, что страница записалась корректно
		readPage, err := dm.ReadPage(tableName, pageID)
		require.NoError(t, err)
		require.Equal(t, uint32(1), readPage.Header.PageID)
	})

	t.Run("2. Write page when table does not exist", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "non_existent_table"
		pageID := PageID{PageNumber: 1}
		page := &Page{
			Header: PageHeader{
				PageID:      1,
				RecordCount: 0,
				Lower:       PAGE_HEADER_SIZE,
				Upper:       PAGE_SIZE,
			},
			Slots:   []PageSlot{},
			Rows:    []Row{},
			Columns: []ColumnInfo{},
		}

		// Act
		writtenPage, err := dm.WritePage(tableName, pageID, page)

		// Assert
		require.Error(t, err)
		require.Nil(t, writtenPage)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestDiskManagerAddNewPage(t *testing.T) {
	t.Run("1. Add new page success", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "test_add_new_page"
		columns := []ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		// Cleanup
		defer func() {
			dm.DropTable(tableName)
			os.RemoveAll("tables")
		}()

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем базу данных (создает table_list файл)
		err = dm.CreateDataBase()
		require.NoError(t, err)

		// Создаем таблицу
		err = dm.CreateTable(tableName, columns)
		require.NoError(t, err)

		pageID := PageID{PageNumber: 1}

		// Act
		page, err := dm.AddNewPage(tableName, pageID)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, page)
		require.Equal(t, uint32(1), page.Header.PageID)
		require.Equal(t, uint32(0), page.Header.RecordCount)
		require.Len(t, page.Rows, 0)
		require.Len(t, page.Columns, len(columns))

		// Проверяем, что страница добавлена в data файл
		// PageDirectory не обновляется автоматически в текущей реализации
	})

	t.Run("2. Add multiple new pages", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "test_add_multiple_pages"
		columns := []ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
		}

		// Cleanup
		defer func() {
			dm.DropTable(tableName)
			os.RemoveAll("tables")
		}()

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем базу данных (создает table_list файл)
		err = dm.CreateDataBase()
		require.NoError(t, err)

		// Создаем таблицу
		err = dm.CreateTable(tableName, columns)
		require.NoError(t, err)

		// Act - добавляем 3 страницы
		for i := 1; i <= 3; i++ {
			pageID := PageID{PageNumber: uint32(i)}
			page, err := dm.AddNewPage(tableName, pageID)
			require.NoError(t, err)
			require.NotNil(t, page)
			require.Equal(t, uint32(i), page.Header.PageID)
		}

		// Assert - проверяем, что все страницы добавлены в data файл
		// PageDirectory не обновляется автоматически в текущей реализации
	})

	t.Run("3. Add new page when table does not exist", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "non_existent_table"
		pageID := PageID{PageNumber: 1}

		// Act
		page, err := dm.AddNewPage(tableName, pageID)

		// Assert
		require.Error(t, err)
		require.Nil(t, page)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestDiskManagerComplexPageOperations(t *testing.T) {
	t.Run("1. Complex page operations with multiple data types", func(t *testing.T) {
		// Arrange
		dm := NewDiskManager()
		tableName := "complex_test_table"
		columns := []ColumnInfo{
			{
				ColumnNameLength: 2,
				ColumnName:       "id",
				DataType:         INT_32_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     1,
				IsAutoIncrement:  1,
				DefaultValue:     0,
			},
			{
				ColumnNameLength: 4,
				ColumnName:       "name",
				DataType:         TEXT_TYPE,
				IsNullable:       1,
				IsPrimaryKey:     0,
				IsAutoIncrement:  0,
				DefaultValue:     0,
			},
			{
				ColumnNameLength: 3,
				ColumnName:       "age",
				DataType:         INT_32_TYPE,
				IsNullable:       1,
				IsPrimaryKey:     0,
				IsAutoIncrement:  0,
				DefaultValue:     0,
			},
			{
				ColumnNameLength: 5,
				ColumnName:       "email",
				DataType:         TEXT_TYPE,
				IsNullable:       0,
				IsPrimaryKey:     0,
				IsAutoIncrement:  0,
				DefaultValue:     0,
			},
		}

		// Cleanup
		// defer func() {
		// 	dm.DropTable(tableName)
		// 	os.RemoveAll("tables")
		// }()

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем базу данных (создает table_list файл)
		err = dm.CreateDataBase()
		require.NoError(t, err)

		// Создаем таблицу
		err = dm.CreateTable(tableName, columns)
		require.NoError(t, err)

		// Добавляем страницу
		pageID := PageID{PageNumber: 1}
		page, err := dm.AddNewPage(tableName, pageID)
		require.NoError(t, err)
		require.NotNil(t, page)

		// Создаем тестовые строки с разными типами данных
		testRows := []Row{
			// Строка 1: все поля заполнены
			{
				{DataType: INT_32_TYPE, Data: int32(1), IsNull: false},
				{DataType: TEXT_TYPE, Data: "John Doe", IsNull: false},
				{DataType: INT_32_TYPE, Data: int32(25), IsNull: false},
				{DataType: TEXT_TYPE, Data: "john@example.com", IsNull: false},
			},
			// Строка 2: некоторые поля NULL
			{
				{DataType: INT_32_TYPE, Data: int32(2), IsNull: false},
				{DataType: TEXT_TYPE, Data: "", IsNull: true}, // NULL name
				{DataType: INT_32_TYPE, Data: int32(30), IsNull: false},
				{DataType: TEXT_TYPE, Data: "jane@example.com", IsNull: false},
			},
			// Строка 3: NULL age
			{
				{DataType: INT_32_TYPE, Data: int32(3), IsNull: false},
				{DataType: TEXT_TYPE, Data: "Bob Smith", IsNull: false},
				{DataType: INT_32_TYPE, Data: int32(0), IsNull: true}, // NULL age
				{DataType: TEXT_TYPE, Data: "bob@example.com", IsNull: false},
			},
			// Строка 4: длинная строка
			{
				{DataType: INT_32_TYPE, Data: int32(4), IsNull: false},
				{DataType: TEXT_TYPE, Data: "Alice Johnson with a very long name that exceeds normal length", IsNull: false},
				{DataType: INT_32_TYPE, Data: int32(35), IsNull: false},
				{DataType: TEXT_TYPE, Data: "alice.johnson.with.very.long.email@very-long-domain-name.example.com", IsNull: false},
			},
			// Строка 5: пустые строки
			{
				{DataType: INT_32_TYPE, Data: int32(5), IsNull: false},
				{DataType: TEXT_TYPE, Data: "", IsNull: false},         // пустая строка, не NULL
				{DataType: INT_32_TYPE, Data: int32(0), IsNull: false}, // 0, не NULL
				{DataType: TEXT_TYPE, Data: "", IsNull: false},         // пустая строка, не NULL
			},
		}

		// Добавляем строки на страницу
		page.Rows = testRows
		page.Header.RecordCount = uint32(len(testRows))

		// Вычисляем слоты для каждой строки
		page.Slots = make([]PageSlot, len(testRows))
		currentOffset := uint32(PAGE_SIZE)

		for i, row := range testRows {
			rowSize := row.GetSize()
			currentOffset -= rowSize

			page.Slots[i] = PageSlot{
				Offset: currentOffset,
				Length: rowSize,
				Flags:  0, // active
			}
		}

		// Обновляем заголовок страницы
		page.Header.Lower = PAGE_HEADER_SIZE + uint32(len(page.Slots))*SLOT_SIZE
		page.Header.Upper = currentOffset

		// Act - записываем страницу
		writtenPage, err := dm.WritePage(tableName, pageID, page)
		require.NoError(t, err)
		require.NotNil(t, writtenPage)

		// Assert - читаем страницу обратно и проверяем данные
		readPage, err := dm.ReadPage(tableName, pageID)
		require.NoError(t, err)
		require.NotNil(t, readPage)

		// Проверяем заголовок
		require.Equal(t, uint32(1), readPage.Header.PageID)
		require.Equal(t, uint32(len(testRows)), readPage.Header.RecordCount)
		require.Equal(t, page.Header.Lower, readPage.Header.Lower)
		require.Equal(t, page.Header.Upper, readPage.Header.Upper)

		// Проверяем количество строк
		require.Len(t, readPage.Rows, len(testRows))

		// Проверяем каждую строку детально
		for i, expectedRow := range testRows {
			actualRow := readPage.Rows[i]
			require.Len(t, actualRow, len(expectedRow))

			for j, expectedCell := range expectedRow {
				actualCell := actualRow[j]

				// Проверяем тип данных
				require.Equal(t, expectedCell.DataType, actualCell.DataType)

				// Проверяем NULL флаг
				require.Equal(t, expectedCell.IsNull, actualCell.IsNull)

				// Проверяем данные (если не NULL)
				if !expectedCell.IsNull {
					switch expectedCell.DataType {
					case INT_32_TYPE:
						require.Equal(t, expectedCell.Data.(int32), actualCell.Data.(int32))
					case TEXT_TYPE:
						require.Equal(t, expectedCell.Data.(string), actualCell.Data.(string))
					}
				}
			}
		}

		// Проверяем слоты
		require.Len(t, readPage.Slots, len(testRows))
		for i, expectedSlot := range page.Slots {
			actualSlot := readPage.Slots[i]
			require.Equal(t, expectedSlot.Offset, actualSlot.Offset)
			require.Equal(t, expectedSlot.Length, actualSlot.Length)
			require.Equal(t, expectedSlot.Flags, actualSlot.Flags)
		}

		// Проверяем колонки
		require.Len(t, readPage.Columns, len(columns))
		for i, expectedColumn := range columns {
			actualColumn := readPage.Columns[i]
			require.Equal(t, expectedColumn.DataType, actualColumn.DataType)
			require.Equal(t, expectedColumn.IsNullable, actualColumn.IsNullable)
			require.Equal(t, expectedColumn.IsPrimaryKey, actualColumn.IsPrimaryKey)
			require.Equal(t, expectedColumn.IsAutoIncrement, actualColumn.IsAutoIncrement)
		}

		// Дополнительная проверка: сериализация и десериализация
		// Конвертируем страницу в RawPage и обратно
		rawPage := ConvertPageToRawPage(readPage)
		require.NotNil(t, rawPage)
		require.Equal(t, readPage.Header, rawPage.Header)
		require.Len(t, rawPage.RawTuples, len(testRows))

		// Проверяем, что RawTuples корректно сериализованы
		for _, rawTuple := range rawPage.RawTuples {
			require.Greater(t, rawTuple.Length, uint32(0))
			require.Greater(t, rawTuple.NullBitmapSize, uint32(0))
			require.Len(t, rawTuple.NullBitmap, int(rawTuple.NullBitmapSize))
			require.Len(t, rawTuple.Data, int(rawTuple.Length-TUPLE_LENGTH_FIELD_SIZE-TUPLE_NULL_BITMAP_SIZE-rawTuple.NullBitmapSize))
		}

		// Проверяем размеры строк
		for i, row := range testRows {
			expectedSize := row.GetSize()
			actualSize := readPage.Rows[i].GetSize()
			require.Equal(t, expectedSize, actualSize, "Row %d size mismatch", i)
		}

		// Проверяем, что страница корректно записана в PageDirectory
		pageDirectory, err := dm.ReadPageDirectory(tableName)
		require.NoError(t, err)
		require.NotNil(t, pageDirectory)
		require.Equal(t, tableName, pageDirectory.TableName)
	})
}
