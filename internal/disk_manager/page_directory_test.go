package disk_manager

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPageDirectoryHeader_Serialize(t *testing.T) {
	t.Run("should serialize page directory header correctly", func(t *testing.T) {
		// Arrange
		header := NewPageDirectoryHeader(3)
		header.NextPageID = 42

		// Act
		result := header.Serialize()

		// Assert
		require.NotNil(t, result)
		assert.Equal(t, 12, len(result), "Serialized data should be 12 bytes")

		// Проверяем MagicNumber (первые 4 байта)
		assert.Equal(t, uint32(PAGE_DIRECTORY_MAGIC_NUMBER), binary.BigEndian.Uint32(result[0:4]))

		// Проверяем PageCount (байты 4-8)
		assert.Equal(t, uint32(3), binary.BigEndian.Uint32(result[4:8]))

		// Проверяем NextPageID (байты 8-12)
		assert.Equal(t, uint32(42), binary.BigEndian.Uint32(result[8:12]))
	})

	t.Run("should serialize with zero values", func(t *testing.T) {
		// Arrange
		header := NewPageDirectoryHeader(0)

		// Act
		result := header.Serialize()

		// Assert
		require.NotNil(t, result)
		assert.Equal(t, 12, len(result))

		// Проверяем MagicNumber
		assert.Equal(t, uint32(PAGE_DIRECTORY_MAGIC_NUMBER), binary.BigEndian.Uint32(result[0:4]))

		// Проверяем PageCount
		assert.Equal(t, uint32(0), binary.BigEndian.Uint32(result[4:8]))

		// Проверяем NextPageID
		assert.Equal(t, uint32(0), binary.BigEndian.Uint32(result[8:12]))
	})

	t.Run("should serialize with maximum values", func(t *testing.T) {
		// Arrange
		header := NewPageDirectoryHeader(0xFFFFFFFF)
		header.NextPageID = 0xFFFFFFFF

		// Act
		result := header.Serialize()

		// Assert
		require.NotNil(t, result)
		assert.Equal(t, 12, len(result))

		// Проверяем максимальные значения
		assert.Equal(t, uint32(PAGE_DIRECTORY_MAGIC_NUMBER), binary.BigEndian.Uint32(result[0:4]))
		assert.Equal(t, uint32(0xFFFFFFFF), binary.BigEndian.Uint32(result[4:8]))
		assert.Equal(t, uint32(0xFFFFFFFF), binary.BigEndian.Uint32(result[8:12]))
	})
}

func TestPageDirectoryHeader_Deserialize(t *testing.T) {
	t.Run("should deserialize page directory header correctly", func(t *testing.T) {
		// Arrange
		data := make([]byte, 12)
		binary.BigEndian.PutUint32(data[0:4], PAGE_DIRECTORY_MAGIC_NUMBER)
		binary.BigEndian.PutUint32(data[4:8], 3)
		binary.BigEndian.PutUint32(data[8:12], 42)

		// Act
		result, err := (&PageDirectoryHeader{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, uint32(PAGE_DIRECTORY_MAGIC_NUMBER), result.MagicNumber)
		assert.Equal(t, uint32(3), result.PageCount)
		assert.Equal(t, uint32(42), result.NextPageID)
	})

	t.Run("should deserialize with zero values", func(t *testing.T) {
		// Arrange
		data := make([]byte, 12)
		// Все байты уже равны 0

		// Act
		result, err := (&PageDirectoryHeader{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, uint32(0), result.MagicNumber)
		assert.Equal(t, uint32(0), result.PageCount)
		assert.Equal(t, uint32(0), result.NextPageID)
	})

	t.Run("should return error for insufficient data", func(t *testing.T) {
		// Arrange
		data := make([]byte, 11) // Меньше 12 байт

		// Act
		result, err := (&PageDirectoryHeader{}).Deserialize(data)

		// Assert
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "insufficient data for page directory header")
	})
}

func TestPageDirectoryHeader_SerializeDeserialize_RoundTrip(t *testing.T) {
	t.Run("should maintain data integrity through serialize-deserialize", func(t *testing.T) {
		// Arrange
		original := NewPageDirectoryHeader(3)
		original.NextPageID = 42

		// Act
		serialized := original.Serialize()
		deserialized, err := (&PageDirectoryHeader{}).Deserialize(serialized)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, deserialized)

		assert.Equal(t, original.MagicNumber, deserialized.MagicNumber)
		assert.Equal(t, original.PageCount, deserialized.PageCount)
		assert.Equal(t, original.NextPageID, deserialized.NextPageID)
	})
}

func TestPageDirectoryEntry_Serialize(t *testing.T) {
	t.Run("should serialize page directory entry correctly", func(t *testing.T) {
		// Arrange
		entry := NewPageDirectoryEntry(1, 1024, 2)

		// Act
		result := entry.Serialize()

		// Assert
		require.NotNil(t, result)
		assert.Equal(t, 12, len(result), "Serialized data should be 12 bytes")

		// Проверяем PageID (байты 0-4)
		assert.Equal(t, uint32(1), binary.BigEndian.Uint32(result[0:4]))

		// Проверяем FreeSpace (байты 4-8)
		assert.Equal(t, uint32(1024), binary.BigEndian.Uint32(result[4:8]))

		// Проверяем Flags (байты 8-12)
		assert.Equal(t, uint32(2), binary.BigEndian.Uint32(result[8:12]))
	})

	t.Run("should serialize with zero values", func(t *testing.T) {
		// Arrange
		entry := NewPageDirectoryEntry(0, 0, 0)

		// Act
		result := entry.Serialize()

		// Assert
		require.NotNil(t, result)
		assert.Equal(t, 12, len(result))

		// Проверяем, что все поля равны нулю
		assert.Equal(t, uint32(0), binary.BigEndian.Uint32(result[0:4]))
		assert.Equal(t, uint32(0), binary.BigEndian.Uint32(result[4:8]))
		assert.Equal(t, uint32(0), binary.BigEndian.Uint32(result[8:12]))
	})
}

func TestPageDirectoryEntry_Deserialize(t *testing.T) {
	t.Run("should deserialize page directory entry correctly", func(t *testing.T) {
		// Arrange
		data := make([]byte, 12)
		binary.BigEndian.PutUint32(data[0:4], 1)
		binary.BigEndian.PutUint32(data[4:8], 1024)
		binary.BigEndian.PutUint32(data[8:12], 2)

		// Act
		result, err := (&PageDirectoryEntry{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, uint32(1), result.PageID)
		assert.Equal(t, uint32(1024), result.FreeSpace)
		assert.Equal(t, uint32(2), result.Flags)
	})

	t.Run("should return error for insufficient data", func(t *testing.T) {
		// Arrange
		data := make([]byte, 11) // Меньше 12 байт

		// Act
		result, err := (&PageDirectoryEntry{}).Deserialize(data)

		// Assert
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "insufficient data for page directory entry")
	})
}

func TestPageDirectoryEntry_SerializeDeserialize_RoundTrip(t *testing.T) {
	t.Run("should maintain data integrity through serialize-deserialize", func(t *testing.T) {
		// Arrange
		original := NewPageDirectoryEntry(1, 1024, 2)

		// Act
		serialized := original.Serialize()
		deserialized, err := (&PageDirectoryEntry{}).Deserialize(serialized)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, deserialized)

		assert.Equal(t, original.PageID, deserialized.PageID)
		assert.Equal(t, original.FreeSpace, deserialized.FreeSpace)
		assert.Equal(t, original.Flags, deserialized.Flags)
	})
}

func TestCreatePageDirectory(t *testing.T) {
	t.Run("should create page directory with single entry", func(t *testing.T) {
		// Arrange
		tableName := "test_users"
		entries := []PageDirectoryEntry{
			*NewPageDirectoryEntry(0, 1024, 1),
		}
		dirFilePath := filepath.Join("tables", tableName+".dir")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(dirFilePath)
			os.Remove("tables")
		}()

		// Act
		pageDir, err := CreatePageDirectory(tableName, entries)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, pageDir)
		require.NotNil(t, pageDir.Header)
		require.Len(t, pageDir.Entries, 1)

		// Check file exists
		_, err = os.Stat(dirFilePath)
		require.NoError(t, err, "page directory file should exist")

		// Check header
		assert.Equal(t, uint32(PAGE_DIRECTORY_MAGIC_NUMBER), pageDir.Header.MagicNumber)
		assert.Equal(t, uint32(1), pageDir.Header.PageCount)
		assert.Equal(t, uint32(1), pageDir.Header.NextPageID)

		// Check entry
		assert.Equal(t, uint32(0), pageDir.Entries[0].PageID)
		assert.Equal(t, uint32(1024), pageDir.Entries[0].FreeSpace)
		assert.Equal(t, uint32(1), pageDir.Entries[0].Flags)
	})

	t.Run("should create page directory with multiple entries", func(t *testing.T) {
		// Arrange
		tableName := "test_products"
		entries := []PageDirectoryEntry{
			*NewPageDirectoryEntry(0, 1024, 1),
			*NewPageDirectoryEntry(1, 2048, 2),
			*NewPageDirectoryEntry(2, 512, 0),
		}
		dirFilePath := filepath.Join("tables", tableName+".dir")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(dirFilePath)
			os.Remove("tables")
		}()

		// Act
		pageDir, err := CreatePageDirectory(tableName, entries)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, pageDir)
		require.NotNil(t, pageDir.Header)
		require.Len(t, pageDir.Entries, 3)

		// Check file exists
		_, err = os.Stat(dirFilePath)
		require.NoError(t, err, "page directory file should exist")

		// Check header
		assert.Equal(t, uint32(PAGE_DIRECTORY_MAGIC_NUMBER), pageDir.Header.MagicNumber)
		assert.Equal(t, uint32(3), pageDir.Header.PageCount)

		// Check entries
		assert.Equal(t, uint32(0), pageDir.Entries[0].PageID)
		assert.Equal(t, uint32(1024), pageDir.Entries[0].FreeSpace)
		assert.Equal(t, uint32(1), pageDir.Entries[0].Flags)

		assert.Equal(t, uint32(1), pageDir.Entries[1].PageID)
		assert.Equal(t, uint32(2048), pageDir.Entries[1].FreeSpace)
		assert.Equal(t, uint32(2), pageDir.Entries[1].Flags)

		assert.Equal(t, uint32(2), pageDir.Entries[2].PageID)
		assert.Equal(t, uint32(512), pageDir.Entries[2].FreeSpace)
		assert.Equal(t, uint32(0), pageDir.Entries[2].Flags)
	})

	t.Run("should create page directory with empty entries", func(t *testing.T) {
		// Arrange
		tableName := "empty_table"
		entries := []PageDirectoryEntry{}
		dirFilePath := filepath.Join("tables", tableName+".dir")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(dirFilePath)
			os.Remove("tables")
		}()

		// Act
		pageDir, err := CreatePageDirectory(tableName, entries)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, pageDir)
		require.NotNil(t, pageDir.Header)
		require.Len(t, pageDir.Entries, 0)

		// Check file exists
		_, err = os.Stat(dirFilePath)
		require.NoError(t, err, "page directory file should exist")

		// Check header
		assert.Equal(t, uint32(PAGE_DIRECTORY_MAGIC_NUMBER), pageDir.Header.MagicNumber)
		assert.Equal(t, uint32(0), pageDir.Header.PageCount)
		assert.Equal(t, uint32(0), pageDir.Header.NextPageID)
	})

	t.Run("should verify file content structure", func(t *testing.T) {
		// Arrange
		tableName := "verify_structure"
		entries := []PageDirectoryEntry{
			*NewPageDirectoryEntry(0, 1024, 1),
			*NewPageDirectoryEntry(1, 2048, 2),
		}
		dirFilePath := filepath.Join("tables", tableName+".dir")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(dirFilePath)
			os.Remove("tables")
		}()

		// Act
		_, err = CreatePageDirectory(tableName, entries)

		// Assert
		require.NoError(t, err)

		// Read file content
		fileContent, err := os.ReadFile(dirFilePath)
		require.NoError(t, err)

		// Check file size
		expectedSize := 12 + (12 * 2) // Header + 2 entries
		assert.Equal(t, expectedSize, len(fileContent), "file size should match expected")

		// Verify header content
		assert.Equal(t, uint32(PAGE_DIRECTORY_MAGIC_NUMBER), binary.BigEndian.Uint32(fileContent[0:4]))
		assert.Equal(t, uint32(2), binary.BigEndian.Uint32(fileContent[4:8]))
		assert.Equal(t, uint32(2), binary.BigEndian.Uint32(fileContent[8:12]))

		// Verify first entry content
		firstEntryStart := 12
		assert.Equal(t, uint32(0), binary.BigEndian.Uint32(fileContent[firstEntryStart:firstEntryStart+4]))
		assert.Equal(t, uint32(1024), binary.BigEndian.Uint32(fileContent[firstEntryStart+4:firstEntryStart+8]))
		assert.Equal(t, uint32(1), binary.BigEndian.Uint32(fileContent[firstEntryStart+8:firstEntryStart+12]))

		// Verify second entry content
		secondEntryStart := 12 + 12
		assert.Equal(t, uint32(1), binary.BigEndian.Uint32(fileContent[secondEntryStart:secondEntryStart+4]))
		assert.Equal(t, uint32(2048), binary.BigEndian.Uint32(fileContent[secondEntryStart+4:secondEntryStart+8]))
		assert.Equal(t, uint32(2), binary.BigEndian.Uint32(fileContent[secondEntryStart+8:secondEntryStart+12]))
	})

	t.Run("should handle file creation error", func(t *testing.T) {
		// Arrange
		tableName := "invalid/path/table"
		entries := []PageDirectoryEntry{
			*NewPageDirectoryEntry(0, 1024, 1),
		}

		// Act
		pageDir, err := CreatePageDirectory(tableName, entries)

		// Assert
		require.Error(t, err)
		assert.Nil(t, pageDir)
		assert.Contains(t, err.Error(), "failed to create page directory file")
	})
}

func TestReadPageDirectory(t *testing.T) {
	t.Run("should read page directory with single entry", func(t *testing.T) {
		// Arrange
		tableName := "test_read_users"
		entries := []PageDirectoryEntry{
			*NewPageDirectoryEntry(0, 1024, 1),
		}
		dirFilePath := filepath.Join("tables", tableName+".dir")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(dirFilePath)
			os.Remove("tables")
		}()

		// Create page directory first
		createdPageDir, err := CreatePageDirectory(tableName, entries)
		require.NoError(t, err)
		require.NotNil(t, createdPageDir)

		// Act
		readPageDir, err := ReadPageDirectory(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, readPageDir)
		require.NotNil(t, readPageDir.Header)
		require.Len(t, readPageDir.Entries, 1)

		// Check header
		assert.Equal(t, uint32(PAGE_DIRECTORY_MAGIC_NUMBER), readPageDir.Header.MagicNumber)
		assert.Equal(t, uint32(1), readPageDir.Header.PageCount)
		assert.Equal(t, uint32(1), readPageDir.Header.NextPageID)

		// Check entry
		assert.Equal(t, uint32(0), readPageDir.Entries[0].PageID)
		assert.Equal(t, uint32(1024), readPageDir.Entries[0].FreeSpace)
		assert.Equal(t, uint32(1), readPageDir.Entries[0].Flags)
	})

	t.Run("should read page directory with multiple entries", func(t *testing.T) {
		// Arrange
		tableName := "test_read_products"
		entries := []PageDirectoryEntry{
			*NewPageDirectoryEntry(0, 1024, 1),
			*NewPageDirectoryEntry(1, 2048, 2),
			*NewPageDirectoryEntry(2, 512, 0),
		}
		dirFilePath := filepath.Join("tables", tableName+".dir")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(dirFilePath)
			os.Remove("tables")
		}()

		// Create page directory first
		createdPageDir, err := CreatePageDirectory(tableName, entries)
		require.NoError(t, err)
		require.NotNil(t, createdPageDir)

		// Act
		readPageDir, err := ReadPageDirectory(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, readPageDir)
		require.NotNil(t, readPageDir.Header)
		require.Len(t, readPageDir.Entries, 3)

		// Check header
		assert.Equal(t, uint32(PAGE_DIRECTORY_MAGIC_NUMBER), readPageDir.Header.MagicNumber)
		assert.Equal(t, uint32(3), readPageDir.Header.PageCount)

		// Check entries
		assert.Equal(t, uint32(0), readPageDir.Entries[0].PageID)
		assert.Equal(t, uint32(1024), readPageDir.Entries[0].FreeSpace)
		assert.Equal(t, uint32(1), readPageDir.Entries[0].Flags)

		assert.Equal(t, uint32(1), readPageDir.Entries[1].PageID)
		assert.Equal(t, uint32(2048), readPageDir.Entries[1].FreeSpace)
		assert.Equal(t, uint32(2), readPageDir.Entries[1].Flags)

		assert.Equal(t, uint32(2), readPageDir.Entries[2].PageID)
		assert.Equal(t, uint32(512), readPageDir.Entries[2].FreeSpace)
		assert.Equal(t, uint32(0), readPageDir.Entries[2].Flags)
	})

	t.Run("should return error for non-existent table", func(t *testing.T) {
		// Arrange
		tableName := "non_existent_table"

		// Act
		readPageDir, err := ReadPageDirectory(tableName)

		// Assert
		require.Error(t, err)
		assert.Nil(t, readPageDir)
		assert.Contains(t, err.Error(), "page directory for table non_existent_table not found")
	})

	t.Run("should return error for corrupted header", func(t *testing.T) {
		// Arrange
		tableName := "test_corrupted_header"
		dirFilePath := filepath.Join("tables", tableName+".dir")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(dirFilePath)
			os.Remove("tables")
		}()

		// Create a corrupted file with insufficient header data
		file, err := os.Create(dirFilePath)
		require.NoError(t, err)

		// Write only 10 bytes instead of 12
		corruptedData := make([]byte, 10)
		_, err = file.Write(corruptedData)
		require.NoError(t, err)
		file.Close()

		// Act
		readPageDir, err := ReadPageDirectory(tableName)

		// Assert
		require.Error(t, err)
		assert.Nil(t, readPageDir)
		assert.Contains(t, err.Error(), "incomplete header read")
	})

	t.Run("should verify data integrity through create-read cycle", func(t *testing.T) {
		// Arrange
		tableName := "test_integrity"
		originalEntries := []PageDirectoryEntry{
			*NewPageDirectoryEntry(0, 1024, 1),
			*NewPageDirectoryEntry(1, 2048, 2),
			*NewPageDirectoryEntry(2, 512, 0),
		}
		dirFilePath := filepath.Join("tables", tableName+".dir")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(dirFilePath)
			os.Remove("tables")
		}()

		// Create page directory
		createdPageDir, err := CreatePageDirectory(tableName, originalEntries)
		require.NoError(t, err)
		require.NotNil(t, createdPageDir)

		// Act - Read page directory
		readPageDir, err := ReadPageDirectory(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, readPageDir)

		// Verify header integrity
		assert.Equal(t, createdPageDir.Header.MagicNumber, readPageDir.Header.MagicNumber)
		assert.Equal(t, createdPageDir.Header.PageCount, readPageDir.Header.PageCount)
		assert.Equal(t, createdPageDir.Header.NextPageID, readPageDir.Header.NextPageID)

		// Verify entries integrity
		require.Len(t, readPageDir.Entries, len(originalEntries))
		for i, originalEntry := range originalEntries {
			readEntry := readPageDir.Entries[i]
			assert.Equal(t, originalEntry.PageID, readEntry.PageID)
			assert.Equal(t, originalEntry.FreeSpace, readEntry.FreeSpace)
			assert.Equal(t, originalEntry.Flags, readEntry.Flags)
		}
	})
}

func TestPageDirectory_AddPage(t *testing.T) {
	t.Run("should add page to empty directory", func(t *testing.T) {
		// Arrange
		tableName := "test_add_page"
		entries := []PageDirectoryEntry{}
		dirFilePath := filepath.Join("tables", tableName+".dir")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(dirFilePath)
			os.Remove("tables")
		}()

		// Create page directory
		pageDir, err := CreatePageDirectory(tableName, entries)
		require.NoError(t, err)
		require.NotNil(t, pageDir)

		// Act
		err = pageDir.AddPage(1024, 1)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, uint32(1), pageDir.Header.PageCount)
		assert.Equal(t, uint32(1), pageDir.Header.NextPageID)
		require.Len(t, pageDir.Entries, 1)

		// Check added entry
		assert.Equal(t, uint32(0), pageDir.Entries[0].PageID)
		assert.Equal(t, uint32(1024), pageDir.Entries[0].FreeSpace)
		assert.Equal(t, uint32(1), pageDir.Entries[0].Flags)
	})

	t.Run("should add page to existing directory", func(t *testing.T) {
		// Arrange
		tableName := "test_add_page_existing"
		entries := []PageDirectoryEntry{
			*NewPageDirectoryEntry(0, 1024, 1),
		}
		dirFilePath := filepath.Join("tables", tableName+".dir")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(dirFilePath)
			os.Remove("tables")
		}()

		// Create page directory
		pageDir, err := CreatePageDirectory(tableName, entries)
		require.NoError(t, err)
		require.NotNil(t, pageDir)

		// Act
		err = pageDir.AddPage(2048, 2)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, uint32(2), pageDir.Header.PageCount)
		assert.Equal(t, uint32(2), pageDir.Header.NextPageID)
		require.Len(t, pageDir.Entries, 2)

		// Check added entry
		assert.Equal(t, uint32(1), pageDir.Entries[1].PageID)
		assert.Equal(t, uint32(2048), pageDir.Entries[1].FreeSpace)
		assert.Equal(t, uint32(2), pageDir.Entries[1].Flags)
	})
}

func TestPageDirectory_UpdatePage(t *testing.T) {
	t.Run("should update existing page", func(t *testing.T) {
		// Arrange
		tableName := "test_update_page"
		entries := []PageDirectoryEntry{
			*NewPageDirectoryEntry(0, 1024, 1),
			*NewPageDirectoryEntry(1, 2048, 2),
		}
		dirFilePath := filepath.Join("tables", tableName+".dir")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(dirFilePath)
			os.Remove("tables")
		}()

		// Create page directory
		pageDir, err := CreatePageDirectory(tableName, entries)
		require.NoError(t, err)
		require.NotNil(t, pageDir)

		// Act
		err = pageDir.UpdatePage(1, 1500, 3)

		// Assert
		require.NoError(t, err)

		// Check updated entry
		assert.Equal(t, uint32(1), pageDir.Entries[1].PageID)
		assert.Equal(t, uint32(1500), pageDir.Entries[1].FreeSpace)
		assert.Equal(t, uint32(3), pageDir.Entries[1].Flags)

		// Check that other entry is unchanged
		assert.Equal(t, uint32(0), pageDir.Entries[0].PageID)
		assert.Equal(t, uint32(1024), pageDir.Entries[0].FreeSpace)
		assert.Equal(t, uint32(1), pageDir.Entries[0].Flags)
	})

	t.Run("should return error for non-existent page", func(t *testing.T) {
		// Arrange
		tableName := "test_update_nonexistent"
		entries := []PageDirectoryEntry{
			*NewPageDirectoryEntry(0, 1024, 1),
		}
		dirFilePath := filepath.Join("tables", tableName+".dir")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(dirFilePath)
			os.Remove("tables")
		}()

		// Create page directory
		pageDir, err := CreatePageDirectory(tableName, entries)
		require.NoError(t, err)
		require.NotNil(t, pageDir)

		// Act
		err = pageDir.UpdatePage(5, 1500, 3)

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "page 5 not found in directory")
	})
}

func TestPageDirectory_FindPageWithSpace(t *testing.T) {
	t.Run("should find page with sufficient space", func(t *testing.T) {
		// Arrange
		tableName := "test_find_space"
		entries := []PageDirectoryEntry{
			*NewPageDirectoryEntry(0, 512, 1),
			*NewPageDirectoryEntry(1, 1024, 2),
			*NewPageDirectoryEntry(2, 2048, 0),
		}
		dirFilePath := filepath.Join("tables", tableName+".dir")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(dirFilePath)
			os.Remove("tables")
		}()

		// Create page directory
		pageDir, err := CreatePageDirectory(tableName, entries)
		require.NoError(t, err)
		require.NotNil(t, pageDir)

		// Act
		foundPage := pageDir.FindPageWithSpace(1000)

		// Assert
		require.NotNil(t, foundPage)
		assert.Equal(t, uint32(1), foundPage.PageID)
		assert.Equal(t, uint32(1024), foundPage.FreeSpace)
		assert.Equal(t, uint32(2), foundPage.Flags)
	})

	t.Run("should find page with exact space", func(t *testing.T) {
		// Arrange
		tableName := "test_find_exact_space"
		entries := []PageDirectoryEntry{
			*NewPageDirectoryEntry(0, 512, 1),
			*NewPageDirectoryEntry(1, 1024, 2),
		}
		dirFilePath := filepath.Join("tables", tableName+".dir")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(dirFilePath)
			os.Remove("tables")
		}()

		// Create page directory
		pageDir, err := CreatePageDirectory(tableName, entries)
		require.NoError(t, err)
		require.NotNil(t, pageDir)

		// Act
		foundPage := pageDir.FindPageWithSpace(1024)

		// Assert
		require.NotNil(t, foundPage)
		assert.Equal(t, uint32(1), foundPage.PageID)
		assert.Equal(t, uint32(1024), foundPage.FreeSpace)
	})

	t.Run("should return nil when no page has sufficient space", func(t *testing.T) {
		// Arrange
		tableName := "test_no_space"
		entries := []PageDirectoryEntry{
			*NewPageDirectoryEntry(0, 512, 1),
			*NewPageDirectoryEntry(1, 1024, 2),
		}
		dirFilePath := filepath.Join("tables", tableName+".dir")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(dirFilePath)
			os.Remove("tables")
		}()

		// Create page directory
		pageDir, err := CreatePageDirectory(tableName, entries)
		require.NoError(t, err)
		require.NotNil(t, pageDir)

		// Act
		foundPage := pageDir.FindPageWithSpace(3000)

		// Assert
		assert.Nil(t, foundPage)
	})

	t.Run("should return nil for empty directory", func(t *testing.T) {
		// Arrange
		tableName := "test_empty_directory"
		entries := []PageDirectoryEntry{}
		dirFilePath := filepath.Join("tables", tableName+".dir")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(dirFilePath)
			os.Remove("tables")
		}()

		// Create page directory
		pageDir, err := CreatePageDirectory(tableName, entries)
		require.NoError(t, err)
		require.NotNil(t, pageDir)

		// Act
		foundPage := pageDir.FindPageWithSpace(100)

		// Assert
		assert.Nil(t, foundPage)
	})
}
