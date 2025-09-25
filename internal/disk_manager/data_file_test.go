package disk_manager

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSerializeDataFileHeader(t *testing.T) {
	t.Run("1. Data file header serialization success", func(t *testing.T) {
		// Arrange
		header := &DataFileHeader{
			MagicNumber: DATA_FILE_MAGIC_NUMBER,
			PagesCount:  5,
			RecordCount: 100,
		}

		// Act
		data := header.Serialize()

		// Assert
		require.Len(t, data, DATA_FILE_HEADER_SIZE)
		require.Equal(t, uint32(DATA_FILE_MAGIC_NUMBER), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(5), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(100), binary.BigEndian.Uint32(data[8:12]))
	})

	t.Run("2. Data file header serialization with zero values", func(t *testing.T) {
		// Arrange
		header := &DataFileHeader{
			MagicNumber: DATA_FILE_MAGIC_NUMBER,
			PagesCount:  0,
			RecordCount: 0,
		}

		// Act
		data := header.Serialize()

		// Assert
		require.Len(t, data, DATA_FILE_HEADER_SIZE)
		require.Equal(t, uint32(DATA_FILE_MAGIC_NUMBER), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[8:12]))
	})

	t.Run("3. Data file header serialization with large values", func(t *testing.T) {
		// Arrange
		header := &DataFileHeader{
			MagicNumber: DATA_FILE_MAGIC_NUMBER,
			PagesCount:  1000000,
			RecordCount: 5000000,
		}

		// Act
		data := header.Serialize()

		// Assert
		require.Len(t, data, DATA_FILE_HEADER_SIZE)
		require.Equal(t, uint32(DATA_FILE_MAGIC_NUMBER), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(1000000), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(5000000), binary.BigEndian.Uint32(data[8:12]))
	})
}

func TestDeserializeDataFileHeader(t *testing.T) {
	t.Run("1. Data file header deserialization success", func(t *testing.T) {
		// Arrange
		expectedHeader := &DataFileHeader{
			MagicNumber: DATA_FILE_MAGIC_NUMBER,
			PagesCount:  10,
			RecordCount: 250,
		}
		data := expectedHeader.Serialize()

		// Act
		header, err := (&DataFileHeader{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.Equal(t, expectedHeader.MagicNumber, header.MagicNumber)
		require.Equal(t, expectedHeader.PagesCount, header.PagesCount)
		require.Equal(t, expectedHeader.RecordCount, header.RecordCount)
	})

	t.Run("2. Data file header deserialization with insufficient data", func(t *testing.T) {
		// Arrange
		data := make([]byte, DATA_FILE_HEADER_SIZE-1)

		// Act
		header, err := (&DataFileHeader{}).Deserialize(data)

		// Assert
		require.Error(t, err)
		require.Nil(t, header)
		require.Contains(t, err.Error(), "insufficient data")
	})

	t.Run("3. Data file header deserialization with empty data", func(t *testing.T) {
		// Arrange
		data := make([]byte, 0)

		// Act
		header, err := (&DataFileHeader{}).Deserialize(data)

		// Assert
		require.Error(t, err)
		require.Nil(t, header)
		require.Contains(t, err.Error(), "insufficient data")
	})
}

func TestCreateDataFile(t *testing.T) {
	t.Run("1. Create data file success", func(t *testing.T) {
		// Arrange
		tableName := "test_users"
		dirPath := "tables"
		filePath := filepath.Join(dirPath, tableName+".data")

		// Cleanup
		defer os.RemoveAll(dirPath)

		// Создаем папку
		err := os.MkdirAll(dirPath, 0755)
		require.NoError(t, err)

		// Act
		dataFile, err := createDataFile(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, dataFile)
		require.Equal(t, uint32(DATA_FILE_MAGIC_NUMBER), dataFile.Header.MagicNumber)
		require.Equal(t, uint32(0), dataFile.Header.PagesCount)
		require.Equal(t, uint32(0), dataFile.Header.RecordCount)
		require.Empty(t, dataFile.Pages)

		// Проверяем, что файл создан
		_, err = os.Stat(filePath)
		require.NoError(t, err)
	})

	t.Run("2. Create data file when already exists", func(t *testing.T) {
		// Arrange
		tableName := "test_existing_users"
		dirPath := "tables"
		filePath := filepath.Join(dirPath, tableName+".data")

		// Cleanup
		defer os.RemoveAll(dirPath)

		// Создаем файл заранее
		err := os.MkdirAll(dirPath, 0755)
		require.NoError(t, err)
		_, err = os.Create(filePath)
		require.NoError(t, err)

		// Act
		dataFile, err := createDataFile(tableName)

		// Assert
		require.Error(t, err)
		require.Nil(t, dataFile)
		require.Contains(t, err.Error(), "already exists")
	})

	t.Run("3. Create data file with empty table name", func(t *testing.T) {
		// Arrange
		tableName := ""
		dirPath := "tables"

		// Cleanup
		defer os.RemoveAll(dirPath)

		// Создаем папку
		err := os.MkdirAll(dirPath, 0755)
		require.NoError(t, err)

		// Act
		dataFile, err := createDataFile(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, dataFile)

		// Проверяем, что файл создан с именем ".data"
		filePath := filepath.Join(dirPath, ".data")
		_, err = os.Stat(filePath)
		require.NoError(t, err)
	})
}

func TestReadDataFileHeader(t *testing.T) {
	t.Run("1. Read data file header success", func(t *testing.T) {
		// Arrange
		tableName := "test_read_users"
		dirPath := "tables"

		// Cleanup
		defer os.RemoveAll(dirPath)

		// Создаем файл
		err := os.MkdirAll(dirPath, 0755)
		require.NoError(t, err)

		// Создаем data файл
		dataFile, err := createDataFile(tableName)
		require.NoError(t, err)

		// Act
		readDataFile, err := readDataFileHeader(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, readDataFile)
		require.Equal(t, dataFile.Header.MagicNumber, readDataFile.Header.MagicNumber)
		require.Equal(t, dataFile.Header.PagesCount, readDataFile.Header.PagesCount)
		require.Equal(t, dataFile.Header.RecordCount, readDataFile.Header.RecordCount)
	})

	t.Run("2. Read data file header when table does not exist", func(t *testing.T) {
		// Arrange
		tableName := "non_existent_table"

		// Act
		dataFile, err := readDataFileHeader(tableName)

		// Assert
		require.Error(t, err)
		require.Nil(t, dataFile)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("3. Read data file header with invalid magic number", func(t *testing.T) {
		// Arrange
		tableName := "test_invalid_magic"
		dirPath := "tables"
		filePath := filepath.Join(dirPath, tableName+".data")

		// Cleanup
		defer os.RemoveAll(dirPath)

		// Создаем файл с неправильным magic number
		err := os.MkdirAll(dirPath, 0755)
		require.NoError(t, err)

		file, err := os.Create(filePath)
		require.NoError(t, err)

		// Записываем неправильный magic number
		wrongMagic := make([]byte, DATA_FILE_HEADER_SIZE)
		binary.BigEndian.PutUint32(wrongMagic[0:4], 0x87654321) // Неправильный magic
		_, err = file.Write(wrongMagic)
		require.NoError(t, err)
		file.Close()

		// Act
		dataFile, err := readDataFileHeader(tableName)

		// Assert
		require.Error(t, err)
		require.Nil(t, dataFile)
		require.Contains(t, err.Error(), "invalid magic number")
	})
}

func TestDeleteDataFile(t *testing.T) {
	t.Run("1. Delete data file success", func(t *testing.T) {
		// Arrange
		tableName := "test_delete_users"
		dirPath := "tables"
		filePath := filepath.Join(dirPath, tableName+".data")

		// Cleanup
		defer os.RemoveAll(dirPath)

		// Создаем файл
		err := os.MkdirAll(dirPath, 0755)
		require.NoError(t, err)

		dataFile, err := createDataFile(tableName)
		require.NoError(t, err)
		require.NotNil(t, dataFile)

		// Проверяем, что файл существует
		_, err = os.Stat(filePath)
		require.NoError(t, err)

		// Act
		err = deleteDataFile(tableName)

		// Assert
		require.NoError(t, err)

		// Проверяем, что файл удален
		_, err = os.Stat(filePath)
		require.Error(t, err)
		require.True(t, os.IsNotExist(err))
	})

	t.Run("2. Delete data file when table does not exist", func(t *testing.T) {
		// Arrange
		tableName := "non_existent_table"

		// Act
		err := deleteDataFile(tableName)

		// Assert
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("3. Delete data file with empty table name", func(t *testing.T) {
		// Arrange
		tableName := ""

		// Act
		err := deleteDataFile(tableName)

		// Assert
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestDataFileAddPage(t *testing.T) {
	t.Run("1. Add page to data file success", func(t *testing.T) {
		// Arrange
		tableName := "test_add_page_users"
		dirPath := "tables"
		filePath := filepath.Join(dirPath, tableName+".data")

		// Cleanup
		defer os.RemoveAll(dirPath)

		// Создаем файл
		err := os.MkdirAll(dirPath, 0755)
		require.NoError(t, err)

		dataFile, err := createDataFile(tableName)
		require.NoError(t, err)
		require.Equal(t, uint32(0), dataFile.Header.PagesCount)

		// Act
		err = dataFile.addPage(tableName)

		// Assert
		require.NoError(t, err)
		require.Equal(t, uint32(1), dataFile.Header.PagesCount)

		// Проверяем, что файл увеличился в размере
		fileInfo, err := os.Stat(filePath)
		require.NoError(t, err)
		expectedSize := DATA_FILE_HEADER_SIZE + PAGE_SIZE
		require.Equal(t, int64(expectedSize), fileInfo.Size())
	})

	t.Run("2. Add multiple pages to data file", func(t *testing.T) {
		// Arrange
		tableName := "test_add_multiple_pages"
		dirPath := "tables"
		filePath := filepath.Join(dirPath, tableName+".data")

		// Cleanup
		defer os.RemoveAll(dirPath)

		// Создаем файл
		err := os.MkdirAll(dirPath, 0755)
		require.NoError(t, err)

		dataFile, err := createDataFile(tableName)
		require.NoError(t, err)

		// Act - добавляем 3 страницы
		for i := 0; i < 3; i++ {
			err = dataFile.addPage(tableName)
			require.NoError(t, err)
		}

		// Assert
		require.Equal(t, uint32(3), dataFile.Header.PagesCount)

		// Проверяем размер файла
		fileInfo, err := os.Stat(filePath)
		require.NoError(t, err)
		expectedSize := DATA_FILE_HEADER_SIZE + 3*PAGE_SIZE
		require.Equal(t, int64(expectedSize), fileInfo.Size())
	})

	t.Run("3. Add page when table does not exist", func(t *testing.T) {
		// Arrange
		tableName := "non_existent_table"
		dataFile := &DataFile{
			Header: &DataFileHeader{
				MagicNumber: DATA_FILE_MAGIC_NUMBER,
				PagesCount:  0,
				RecordCount: 0,
			},
			Pages: make([]*RawPage, 0),
		}

		// Act
		err := dataFile.addPage(tableName)

		// Assert
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestDataFileReadPage(t *testing.T) {
	t.Run("1. Read page from data file success", func(t *testing.T) {
		// Arrange
		tableName := "test_read_page_users"
		dirPath := "tables"

		// Cleanup
		defer os.RemoveAll(dirPath)

		// Создаем файл
		err := os.MkdirAll(dirPath, 0755)
		require.NoError(t, err)

		dataFile, err := createDataFile(tableName)
		require.NoError(t, err)

		// Добавляем страницу
		err = dataFile.addPage(tableName)
		require.NoError(t, err)

		pageID := PageID{PageNumber: 1}

		// Act
		page, err := dataFile.readPage(tableName, pageID)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, page)
		require.Equal(t, uint32(1), page.Header.PageID)
		require.Equal(t, uint32(0), page.Header.RecordCount)
	})

	t.Run("2. Read page with invalid page ID", func(t *testing.T) {
		// Arrange
		tableName := "test_invalid_page_id"
		dirPath := "tables"

		// Cleanup
		defer os.RemoveAll(dirPath)

		// Создаем файл
		err := os.MkdirAll(dirPath, 0755)
		require.NoError(t, err)

		dataFile, err := createDataFile(tableName)
		require.NoError(t, err)

		// Пытаемся прочитать несуществующую страницу
		pageID := PageID{PageNumber: 5}

		// Act
		page, err := dataFile.readPage(tableName, pageID)

		// Assert
		require.Error(t, err)
		require.Nil(t, page)
		require.Contains(t, err.Error(), "out of range")
	})

	t.Run("3. Read page when table does not exist", func(t *testing.T) {
		// Arrange
		tableName := "non_existent_table"
		dataFile := &DataFile{
			Header: &DataFileHeader{
				MagicNumber: DATA_FILE_MAGIC_NUMBER,
				PagesCount:  1,
				RecordCount: 0,
			},
			Pages: make([]*RawPage, 0),
		}

		pageID := PageID{PageNumber: 1}

		// Act
		page, err := dataFile.readPage(tableName, pageID)

		// Assert
		require.Error(t, err)
		require.Nil(t, page)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestDataFileWritePage(t *testing.T) {
	t.Run("1. Write page to data file success", func(t *testing.T) {
		// Arrange
		tableName := "test_write_page_users"
		dirPath := "tables"

		// Cleanup
		defer os.RemoveAll(dirPath)

		// Создаем файл
		err := os.MkdirAll(dirPath, 0755)
		require.NoError(t, err)

		dataFile, err := createDataFile(tableName)
		require.NoError(t, err)

		// Добавляем страницу
		err = dataFile.addPage(tableName)
		require.NoError(t, err)

		// Создаем новую страницу для записи
		pageID := PageID{PageNumber: 1}
		page := newPage(pageID)
		page.Header.RecordCount = 0 // Оставляем количество записей равным 0

		// Act
		err = dataFile.writePage(tableName, pageID, page)

		// Assert
		require.NoError(t, err)

		// Проверяем, что страница записалась
		readPage, err := dataFile.readPage(tableName, pageID)
		require.NoError(t, err)
		require.Equal(t, uint32(0), readPage.Header.RecordCount)
	})

	t.Run("2. Write page when table does not exist", func(t *testing.T) {
		// Arrange
		tableName := "non_existent_table"
		dataFile := &DataFile{
			Header: &DataFileHeader{
				MagicNumber: DATA_FILE_MAGIC_NUMBER,
				PagesCount:  1,
				RecordCount: 0,
			},
			Pages: make([]*RawPage, 0),
		}

		pageID := PageID{PageNumber: 1}
		page := newPage(pageID)

		// Act
		err := dataFile.writePage(tableName, pageID, page)

		// Assert
		require.Error(t, err)
		require.Contains(t, err.Error(), "no such file or directory")
	})

	t.Run("3. Write page with invalid page ID", func(t *testing.T) {
		// Arrange
		tableName := "test_write_invalid_page"
		dirPath := "tables"

		// Cleanup
		defer os.RemoveAll(dirPath)

		// Создаем файл
		err := os.MkdirAll(dirPath, 0755)
		require.NoError(t, err)

		dataFile, err := createDataFile(tableName)
		require.NoError(t, err)

		// Пытаемся записать в несуществующую страницу
		pageID := PageID{PageNumber: 5}
		page := newPage(pageID)

		// Act
		err = dataFile.writePage(tableName, pageID, page)

		// Assert
		require.NoError(t, err) // Запись должна пройти успешно, даже если страница не существует в заголовке
	})
}
