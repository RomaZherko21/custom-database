package disk_manager

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSerializePageDirectoryHeader(t *testing.T) {
	t.Run("1. Page directory header serialization success", func(t *testing.T) {
		// Arrange
		header := &PageDirectoryHeader{
			MagicNumber: PAGE_DIRECTORY_MAGIC_NUMBER,
			PageCount:   5,
			NextPageID:  10,
		}

		// Act
		data := header.Serialize()

		// Assert
		require.Len(t, data, PAGE_DIRECTORY_HEADER_SIZE)
		require.Equal(t, uint32(PAGE_DIRECTORY_MAGIC_NUMBER), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(5), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(10), binary.BigEndian.Uint32(data[8:12]))
	})

	t.Run("2. Page directory header serialization with zero pages", func(t *testing.T) {
		// Arrange
		header := &PageDirectoryHeader{
			MagicNumber: PAGE_DIRECTORY_MAGIC_NUMBER,
			PageCount:   0,
			NextPageID:  PAGE_INITIAL_ID,
		}

		// Act
		data := header.Serialize()

		// Assert
		require.Len(t, data, PAGE_DIRECTORY_HEADER_SIZE)
		require.Equal(t, uint32(PAGE_DIRECTORY_MAGIC_NUMBER), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(PAGE_INITIAL_ID), binary.BigEndian.Uint32(data[8:12]))
	})

	t.Run("3. Page directory header serialization with large values", func(t *testing.T) {
		// Arrange
		header := &PageDirectoryHeader{
			MagicNumber: PAGE_DIRECTORY_MAGIC_NUMBER,
			PageCount:   1000,
			NextPageID:  5000,
		}

		// Act
		data := header.Serialize()

		// Assert
		require.Len(t, data, PAGE_DIRECTORY_HEADER_SIZE)
		require.Equal(t, uint32(PAGE_DIRECTORY_MAGIC_NUMBER), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(1000), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(5000), binary.BigEndian.Uint32(data[8:12]))
	})
}

func TestDeserializePageDirectoryHeader(t *testing.T) {
	t.Run("1. Page directory header deserialization success", func(t *testing.T) {
		// Arrange
		originalHeader := &PageDirectoryHeader{
			MagicNumber: PAGE_DIRECTORY_MAGIC_NUMBER,
			PageCount:   3,
			NextPageID:  7,
		}
		data := originalHeader.Serialize()

		// Act
		deserializedHeader, err := (&PageDirectoryHeader{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.Equal(t, originalHeader.MagicNumber, deserializedHeader.MagicNumber)
		require.Equal(t, originalHeader.PageCount, deserializedHeader.PageCount)
		require.Equal(t, originalHeader.NextPageID, deserializedHeader.NextPageID)
	})

	t.Run("2. Page directory header deserialization with insufficient data", func(t *testing.T) {
		// Arrange
		insufficientData := make([]byte, PAGE_DIRECTORY_HEADER_SIZE-1)

		// Act
		header, err := (&PageDirectoryHeader{}).Deserialize(insufficientData)

		// Assert
		require.Error(t, err)
		require.Nil(t, header)
		require.Contains(t, err.Error(), "insufficient data for page directory header")
	})

	t.Run("3. Page directory header deserialization with empty data", func(t *testing.T) {
		// Arrange
		emptyData := make([]byte, 0)

		// Act
		header, err := (&PageDirectoryHeader{}).Deserialize(emptyData)

		// Assert
		require.Error(t, err)
		require.Nil(t, header)
		require.Contains(t, err.Error(), "insufficient data for page directory header")
	})
}

func TestSerializePageDirectoryEntry(t *testing.T) {
	t.Run("1. Page directory entry serialization success", func(t *testing.T) {
		// Arrange
		entry := &PageDirectoryEntry{
			PageID:    1,
			FreeSpace: 1024,
			Flags:     0, // активная страница
		}

		// Act
		data := entry.Serialize()

		// Assert
		require.Len(t, data, PAGE_DIRECTORY_ENTRY_SIZE)
		require.Equal(t, uint32(1), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(1024), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[8:12]))
	})

	t.Run("2. Page directory entry serialization with deleted page", func(t *testing.T) {
		// Arrange
		entry := &PageDirectoryEntry{
			PageID:    5,
			FreeSpace: 0,
			Flags:     1, // удаленная страница
		}

		// Act
		data := entry.Serialize()

		// Assert
		require.Len(t, data, PAGE_DIRECTORY_ENTRY_SIZE)
		require.Equal(t, uint32(5), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(1), binary.BigEndian.Uint32(data[8:12]))
	})

	t.Run("3. Page directory entry serialization with full page", func(t *testing.T) {
		// Arrange
		entry := &PageDirectoryEntry{
			PageID:    10,
			FreeSpace: PAGE_SIZE, // полная страница
			Flags:     0,
		}

		// Act
		data := entry.Serialize()

		// Assert
		require.Len(t, data, PAGE_DIRECTORY_ENTRY_SIZE)
		require.Equal(t, uint32(10), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(PAGE_SIZE), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[8:12]))
	})
}

func TestDeserializePageDirectoryEntry(t *testing.T) {
	t.Run("1. Page directory entry deserialization success", func(t *testing.T) {
		// Arrange
		originalEntry := &PageDirectoryEntry{
			PageID:    2,
			FreeSpace: 512,
			Flags:     0,
		}
		data := originalEntry.Serialize()

		// Act
		deserializedEntry, err := (&PageDirectoryEntry{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.Equal(t, originalEntry.PageID, deserializedEntry.PageID)
		require.Equal(t, originalEntry.FreeSpace, deserializedEntry.FreeSpace)
		require.Equal(t, originalEntry.Flags, deserializedEntry.Flags)
	})

	t.Run("2. Page directory entry deserialization with insufficient data", func(t *testing.T) {
		// Arrange
		insufficientData := make([]byte, PAGE_DIRECTORY_ENTRY_SIZE-1)

		// Act
		entry, err := (&PageDirectoryEntry{}).Deserialize(insufficientData)

		// Assert
		require.Error(t, err)
		require.Nil(t, entry)
		require.Contains(t, err.Error(), "insufficient data for page directory entry")
	})

	t.Run("3. Page directory entry deserialization with empty data", func(t *testing.T) {
		// Arrange
		emptyData := make([]byte, 0)

		// Act
		entry, err := (&PageDirectoryEntry{}).Deserialize(emptyData)

		// Assert
		require.Error(t, err)
		require.Nil(t, entry)
		require.Contains(t, err.Error(), "insufficient data for page directory entry")
	})
}

func TestCreatePageDirectoryFile(t *testing.T) {
	t.Run("1. Create page directory file success", func(t *testing.T) {
		// Arrange
		tableName := "test_users"

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Act
		pageDirectory, err := createPageDirectoryFile(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, pageDirectory)
		require.Equal(t, tableName, pageDirectory.TableName)
		require.Equal(t, uint32(PAGE_DIRECTORY_MAGIC_NUMBER), pageDirectory.Header.MagicNumber)
		require.Equal(t, uint32(0), pageDirectory.Header.PageCount)
		require.Equal(t, uint32(PAGE_INITIAL_ID), pageDirectory.Header.NextPageID)
		require.Len(t, pageDirectory.Entries, 0)

		// Проверяем, что файл создался
		dirFilePath := filepath.Join("tables", tableName+".dir")
		_, err = os.Stat(dirFilePath)
		require.NoError(t, err)

		// Cleanup
		err = os.Remove(dirFilePath)
		require.NoError(t, err)
	})

	t.Run("2. Create page directory file when already exists", func(t *testing.T) {
		// Arrange
		tableName := "existing_table"

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем файл заранее
		dirFilePath := filepath.Join("tables", tableName+".dir")
		file, err := os.Create(dirFilePath)
		require.NoError(t, err)
		file.Close()

		// Act
		pageDirectory, err := createPageDirectoryFile(tableName)

		// Assert
		require.Error(t, err)
		require.Nil(t, pageDirectory)
		require.Contains(t, err.Error(), "page directory for table existing_table already exists")

		// Cleanup
		err = os.Remove(dirFilePath)
		require.NoError(t, err)
	})

	t.Run("3. Create page directory file with empty table name", func(t *testing.T) {
		// Arrange
		tableName := ""

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Act
		pageDirectory, err := createPageDirectoryFile(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, pageDirectory)
		require.Equal(t, tableName, pageDirectory.TableName)
		require.Equal(t, uint32(PAGE_DIRECTORY_MAGIC_NUMBER), pageDirectory.Header.MagicNumber)
		require.Equal(t, uint32(0), pageDirectory.Header.PageCount)
		require.Equal(t, uint32(PAGE_INITIAL_ID), pageDirectory.Header.NextPageID)

		// Cleanup
		dirFilePath := filepath.Join("tables", ".dir")
		err = os.Remove(dirFilePath)
		require.NoError(t, err)
	})
}

func TestReadPageDirectory(t *testing.T) {
	t.Run("1. Read page directory file success", func(t *testing.T) {
		// Arrange
		tableName := "read_test_table_unique"

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Cleanup перед тестом
		dirFilePath := filepath.Join("tables", tableName+".dir")
		os.Remove(dirFilePath) // Игнорируем ошибку, если файл не существует

		// Создаем page directory файл
		createdDir, err := createPageDirectoryFile(tableName)
		require.NoError(t, err)

		// Act
		readDir, err := readPageDirectory(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, readDir)
		require.Equal(t, createdDir.TableName, readDir.TableName)
		require.Equal(t, createdDir.Header.MagicNumber, readDir.Header.MagicNumber)
		require.Equal(t, createdDir.Header.PageCount, readDir.Header.PageCount)
		require.Equal(t, createdDir.Header.NextPageID, readDir.Header.NextPageID)
		require.Len(t, readDir.Entries, 0)

		// Cleanup
		err = os.Remove(dirFilePath)
		require.NoError(t, err)
	})

	t.Run("2. Read page directory file when table does not exist", func(t *testing.T) {
		// Arrange
		tableName := "non_existent_table"

		// Act
		pageDirectory, err := readPageDirectory(tableName)

		// Assert
		require.Error(t, err)
		require.Nil(t, pageDirectory)
		require.Contains(t, err.Error(), "page directory for table non_existent_table not found")
	})

	t.Run("3. Read page directory file with invalid magic number", func(t *testing.T) {
		// Arrange
		tableName := "invalid_magic_table"
		dirFilePath := filepath.Join("tables", tableName+".dir")

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем файл с неправильным magic number
		file, err := os.Create(dirFilePath)
		require.NoError(t, err)
		defer file.Close()

		// Записываем неправильный magic number и заполняем остальные байты заголовка
		wrongMagic := make([]byte, PAGE_DIRECTORY_HEADER_SIZE)
		binary.BigEndian.PutUint32(wrongMagic, 0x12345678) // Неправильный magic number
		_, err = file.Write(wrongMagic)
		require.NoError(t, err)

		// Act
		pageDirectory, err := readPageDirectory(tableName)

		// Assert
		require.Error(t, err)
		require.Nil(t, pageDirectory)
		require.Contains(t, err.Error(), "invalid magic number")

		// Cleanup
		err = os.Remove(dirFilePath)
		require.NoError(t, err)
	})

	t.Run("4. Read page directory file with entries", func(t *testing.T) {
		// Arrange
		tableName := "table_with_entries"

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем page directory файл
		createdDir, err := createPageDirectoryFile(tableName)
		require.NoError(t, err)

		// Добавляем entries вручную
		createdDir.Header.PageCount = 2
		createdDir.Entries = []PageDirectoryEntry{
			{PageID: 0, FreeSpace: 1024, Flags: 0},
			{PageID: 1, FreeSpace: 512, Flags: 1},
		}

		// Записываем обновленный page directory
		_, err = writePageDirectory(tableName, createdDir)
		require.NoError(t, err)

		// Act
		readDir, err := readPageDirectory(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, readDir)
		require.Equal(t, uint32(2), readDir.Header.PageCount)
		require.Len(t, readDir.Entries, 2)
		require.Equal(t, uint32(0), readDir.Entries[0].PageID)
		require.Equal(t, uint32(1024), readDir.Entries[0].FreeSpace)
		require.Equal(t, uint32(0), readDir.Entries[0].Flags)
		require.Equal(t, uint32(1), readDir.Entries[1].PageID)
		require.Equal(t, uint32(512), readDir.Entries[1].FreeSpace)
		require.Equal(t, uint32(1), readDir.Entries[1].Flags)

		// Cleanup
		dirFilePath := filepath.Join("tables", tableName+".dir")
		err = os.Remove(dirFilePath)
		require.NoError(t, err)
	})
}

func TestWritePageDirectory(t *testing.T) {
	t.Run("1. Write page directory file success", func(t *testing.T) {
		// Arrange
		tableName := "write_test_table"

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем исходный page directory файл
		originalDir, err := createPageDirectoryFile(tableName)
		require.NoError(t, err)

		// Изменяем данные
		originalDir.Header.PageCount = 3
		originalDir.Header.NextPageID = 15
		originalDir.Entries = []PageDirectoryEntry{
			{PageID: 0, FreeSpace: 1024, Flags: 0},
			{PageID: 1, FreeSpace: 512, Flags: 0},
			{PageID: 2, FreeSpace: 0, Flags: 1},
		}

		// Act
		writtenDir, err := writePageDirectory(tableName, originalDir)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, writtenDir)
		require.Equal(t, uint32(3), writtenDir.Header.PageCount)
		require.Equal(t, uint32(15), writtenDir.Header.NextPageID)
		require.Len(t, writtenDir.Entries, 3)

		// Проверяем, что изменения записались
		readDir, err := readPageDirectory(tableName)
		require.NoError(t, err)
		require.Equal(t, uint32(3), readDir.Header.PageCount)
		require.Equal(t, uint32(15), readDir.Header.NextPageID)
		require.Len(t, readDir.Entries, 3)

		// Cleanup
		dirFilePath := filepath.Join("tables", tableName+".dir")
		err = os.Remove(dirFilePath)
		require.NoError(t, err)
	})

	t.Run("2. Write page directory file when table does not exist", func(t *testing.T) {
		// Arrange
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
		writtenDir, err := writePageDirectory(tableName, pageDirectory)

		// Assert
		require.Error(t, err)
		require.Nil(t, writtenDir)
		require.Contains(t, err.Error(), "page directory for table non_existent_write_table not found")
	})

	t.Run("3. Write page directory file with empty entries", func(t *testing.T) {
		// Arrange
		tableName := "empty_entries_table"

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем исходный page directory файл
		originalDir, err := createPageDirectoryFile(tableName)
		require.NoError(t, err)

		// Очищаем entries
		originalDir.Header.PageCount = 0
		originalDir.Entries = []PageDirectoryEntry{}

		// Act
		writtenDir, err := writePageDirectory(tableName, originalDir)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, writtenDir)
		require.Equal(t, uint32(0), writtenDir.Header.PageCount)
		require.Len(t, writtenDir.Entries, 0)

		// Cleanup
		dirFilePath := filepath.Join("tables", tableName+".dir")
		err = os.Remove(dirFilePath)
		require.NoError(t, err)
	})
}

func TestDeletePageDirectory(t *testing.T) {
	t.Run("1. Delete page directory file success", func(t *testing.T) {
		// Arrange
		tableName := "delete_test_table"

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем page directory файл
		_, err = createPageDirectoryFile(tableName)
		require.NoError(t, err)

		// Проверяем, что файл существует
		dirFilePath := filepath.Join("tables", tableName+".dir")
		_, err = os.Stat(dirFilePath)
		require.NoError(t, err)

		// Act
		err = deletePageDirectory(tableName)

		// Assert
		require.NoError(t, err)

		// Проверяем, что файл удален
		_, err = os.Stat(dirFilePath)
		require.Error(t, err)
		require.True(t, os.IsNotExist(err))
	})

	t.Run("2. Delete page directory file when table does not exist", func(t *testing.T) {
		// Arrange
		tableName := "non_existent_delete_table"

		// Act
		err := deletePageDirectory(tableName)

		// Assert
		require.Error(t, err)
		require.Contains(t, err.Error(), "page directory for table non_existent_delete_table not found")
	})

	t.Run("3. Delete page directory file with empty table name", func(t *testing.T) {
		// Arrange
		tableName := ""

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем page directory файл с пустым именем
		_, err = createPageDirectoryFile(tableName)
		require.NoError(t, err)

		// Проверяем, что файл существует
		dirFilePath := filepath.Join("tables", ".dir")
		_, err = os.Stat(dirFilePath)
		require.NoError(t, err)

		// Act
		err = deletePageDirectory(tableName)

		// Assert
		require.NoError(t, err)

		// Проверяем, что файл удален
		_, err = os.Stat(dirFilePath)
		require.Error(t, err)
		require.True(t, os.IsNotExist(err))
	})
}
