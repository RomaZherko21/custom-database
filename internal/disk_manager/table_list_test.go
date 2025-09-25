package disk_manager

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTablesListHeaderSerialize(t *testing.T) {
	t.Run("1. Serialize header success", func(t *testing.T) {
		// Arrange
		header := &TablesListHeader{
			MagicNumber: TABLES_LIST_MAGIC_NUMBER,
		}

		// Act
		data := header.Serialize()

		// Assert
		require.NotNil(t, data)
		require.Equal(t, TABLES_LIST_HEADER_SIZE, len(data))
		require.Equal(t, TABLES_LIST_MAGIC_NUMBER, int(binary.BigEndian.Uint32(data[0:4])))
	})

	t.Run("2. Serialize header with custom magic number", func(t *testing.T) {
		// Arrange
		customMagic := uint32(0x12345678)
		header := &TablesListHeader{
			MagicNumber: customMagic,
		}

		// Act
		data := header.Serialize()

		// Assert
		require.NotNil(t, data)
		require.Equal(t, TABLES_LIST_HEADER_SIZE, len(data))
		require.Equal(t, customMagic, binary.BigEndian.Uint32(data[0:4]))
	})
}

func TestTablesListHeaderDeserialize(t *testing.T) {
	t.Run("1. Deserialize header success", func(t *testing.T) {
		// Arrange
		data := make([]byte, TABLES_LIST_HEADER_SIZE)
		binary.BigEndian.PutUint32(data[0:4], TABLES_LIST_MAGIC_NUMBER)

		header := &TablesListHeader{}

		// Act
		result, err := header.Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, TABLES_LIST_MAGIC_NUMBER, int(result.MagicNumber))
	})

	t.Run("2. Deserialize header with insufficient data", func(t *testing.T) {
		// Arrange
		data := make([]byte, 2) // Недостаточно данных
		header := &TablesListHeader{}

		// Act
		result, err := header.Deserialize(data)

		// Assert
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "insufficient data")
	})
}

func TestTableEntrySerialize(t *testing.T) {
	t.Run("1. Serialize entry success", func(t *testing.T) {
		// Arrange
		tableName := "users"
		entry := &TableEntry{
			TableNameLength: uint32(len(tableName)),
			FileID:          FileID{FileID: 123},
		}
		copy(entry.TableName[:], tableName)

		// Act
		data := entry.Serialize()

		// Assert
		require.NotNil(t, data)
		require.Equal(t, TABLES_LIST_ENTRY_SIZE, len(data))
		require.Equal(t, uint32(len(tableName)), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, tableName, strings.TrimRight(string(data[4:4+TABLE_NAME_MAX_LENGTH]), "\x00"))
		require.Equal(t, uint32(123), binary.BigEndian.Uint32(data[36:40]))
	})

	t.Run("2. Serialize entry with long table name", func(t *testing.T) {
		// Arrange
		tableName := "very_long_table_name_that_exceeds_limit"
		// Обрезаем имя таблицы до максимальной длины
		truncatedName := tableName[:TABLE_NAME_MAX_LENGTH]
		entry := &TableEntry{
			TableNameLength: uint32(len(truncatedName)),
			FileID:          FileID{FileID: 456},
		}
		copy(entry.TableName[:], truncatedName)

		// Act
		data := entry.Serialize()

		// Assert
		require.NotNil(t, data)
		require.Equal(t, TABLES_LIST_ENTRY_SIZE, len(data))
		require.Equal(t, uint32(len(truncatedName)), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(456), binary.BigEndian.Uint32(data[36:40]))
	})
}

func TestTableEntryDeserialize(t *testing.T) {
	t.Run("1. Deserialize entry success", func(t *testing.T) {
		// Arrange
		tableName := "products"
		data := make([]byte, TABLES_LIST_ENTRY_SIZE)
		binary.BigEndian.PutUint32(data[0:4], uint32(len(tableName)))
		copy(data[4:4+TABLE_NAME_MAX_LENGTH], tableName)
		binary.BigEndian.PutUint32(data[36:40], 789)

		entry := &TableEntry{}

		// Act
		result, err := entry.Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, uint32(len(tableName)), result.TableNameLength)
		require.Equal(t, tableName, strings.TrimRight(string(result.TableName[:]), "\x00"))
		require.Equal(t, uint32(789), result.FileID.FileID)
	})

	t.Run("2. Deserialize entry with insufficient data", func(t *testing.T) {
		// Arrange
		data := make([]byte, 10) // Недостаточно данных
		entry := &TableEntry{}

		// Act
		result, err := entry.Deserialize(data)

		// Assert
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "insufficient data")
	})
}

func TestNewTablesList(t *testing.T) {
	t.Run("1. Create new tables list success", func(t *testing.T) {
		// Act
		tablesList := NewTablesList()

		// Assert
		require.NotNil(t, tablesList)
		require.NotNil(t, tablesList.Header)
		require.Equal(t, TABLES_LIST_MAGIC_NUMBER, int(tablesList.Header.MagicNumber))
		require.NotNil(t, tablesList.Tables)
		require.Equal(t, 0, len(tablesList.Tables))
	})
}

func TestTablesListSerialize(t *testing.T) {
	t.Run("1. Serialize empty tables list", func(t *testing.T) {
		// Arrange
		tablesList := NewTablesList()

		// Act
		data := tablesList.Serialize()

		// Assert
		require.NotNil(t, data)
		require.Equal(t, TABLES_LIST_HEADER_SIZE, len(data))
		require.Equal(t, TABLES_LIST_MAGIC_NUMBER, int(binary.BigEndian.Uint32(data[0:4])))
	})

	t.Run("2. Serialize tables list with entries", func(t *testing.T) {
		// Arrange
		tablesList := NewTablesList()
		tablesList.Tables["users"] = FileID{FileID: 1}
		tablesList.Tables["products"] = FileID{FileID: 2}

		// Act
		data := tablesList.Serialize()

		// Assert
		require.NotNil(t, data)
		expectedSize := TABLES_LIST_HEADER_SIZE + 2*TABLES_LIST_ENTRY_SIZE
		require.Equal(t, expectedSize, len(data))
		require.Equal(t, TABLES_LIST_MAGIC_NUMBER, int(binary.BigEndian.Uint32(data[0:4])))
	})
}

func TestTablesListDeserialize(t *testing.T) {
	t.Run("1. Deserialize empty tables list", func(t *testing.T) {
		// Arrange
		originalList := NewTablesList()
		data := originalList.Serialize()

		tablesList := &TablesList{}

		// Act
		err := tablesList.Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, tablesList.Header)
		require.Equal(t, TABLES_LIST_MAGIC_NUMBER, int(tablesList.Header.MagicNumber))
		require.Equal(t, 0, len(tablesList.Tables))
	})

	t.Run("2. Deserialize tables list with entries", func(t *testing.T) {
		// Arrange
		originalList := NewTablesList()
		originalList.Tables["users"] = FileID{FileID: 1}
		originalList.Tables["orders"] = FileID{FileID: 3}
		data := originalList.Serialize()

		tablesList := &TablesList{}

		// Act
		err := tablesList.Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, tablesList.Header)
		require.Equal(t, TABLES_LIST_MAGIC_NUMBER, int(tablesList.Header.MagicNumber))
		require.Equal(t, 2, len(tablesList.Tables))
		require.Equal(t, uint32(1), tablesList.Tables["users"].FileID)
		require.Equal(t, uint32(3), tablesList.Tables["orders"].FileID)
	})

	t.Run("3. Deserialize with insufficient data", func(t *testing.T) {
		// Arrange
		data := make([]byte, 2) // Недостаточно данных
		tablesList := &TablesList{}

		// Act
		err := tablesList.Deserialize(data)

		// Assert
		require.Error(t, err)
		require.Contains(t, err.Error(), "insufficient data")
	})

	t.Run("4. Deserialize with invalid magic number", func(t *testing.T) {
		// Arrange
		data := make([]byte, TABLES_LIST_HEADER_SIZE)
		binary.BigEndian.PutUint32(data[0:4], 0xDEADBEEF) // Неправильный magic number

		tablesList := &TablesList{}

		// Act
		err := tablesList.Deserialize(data)

		// Assert
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid table list magic number")
	})
}

func TestCreateTableListFile(t *testing.T) {
	t.Run("1. Create table list file success", func(t *testing.T) {
		// Cleanup
		defer func() {
			os.RemoveAll("tables")
		}()

		// Удаляем файл если он существует
		os.Remove(TABLE_LIST_FILE_PATH)

		// Act
		tablesList, err := createTableListFile()

		// Assert
		require.NoError(t, err)
		require.NotNil(t, tablesList)
		require.Equal(t, TABLES_LIST_MAGIC_NUMBER, int(tablesList.Header.MagicNumber))
		require.Equal(t, 0, len(tablesList.Tables))

		// Проверяем, что файл создан
		_, err = os.Stat(TABLE_LIST_FILE_PATH)
		require.NoError(t, err)

		// Cleanup
		os.RemoveAll("tables")
	})

	t.Run("2. Create table list file when already exists", func(t *testing.T) {
		// Arrange
		defer func() {
			os.RemoveAll("tables")
		}()

		// Создаем файл первый раз
		_, err := createTableListFile()
		require.NoError(t, err)

		// Act - пытаемся создать файл второй раз
		tablesList, err := createTableListFile()

		// Assert
		require.Error(t, err)
		require.Nil(t, tablesList)
		require.Contains(t, err.Error(), "already exists")

		// Cleanup
		os.RemoveAll("tables")
	})
}

func TestReadTableListFile(t *testing.T) {
	t.Run("1. Read table list file success", func(t *testing.T) {
		// Arrange
		defer func() {
			os.RemoveAll("tables")
		}()

		// Создаем файл
		originalList, err := createTableListFile()
		require.NoError(t, err)

		// Act
		readList, err := readTableListFile()

		// Assert
		require.NoError(t, err)
		require.NotNil(t, readList)
		require.Equal(t, originalList.Header.MagicNumber, readList.Header.MagicNumber)
		require.Equal(t, len(originalList.Tables), len(readList.Tables))

		// Cleanup
		os.RemoveAll("tables")
	})

	t.Run("2. Read table list file when not exists", func(t *testing.T) {
		// Act
		tablesList, err := readTableListFile()

		// Assert
		require.Error(t, err)
		require.Nil(t, tablesList)
		require.True(t, os.IsNotExist(err))

		// Cleanup
		os.RemoveAll("tables")
	})
}

func TestWriteTablesListFile(t *testing.T) {
	t.Run("1. Write tables list file success", func(t *testing.T) {
		// Arrange
		defer func() {
			os.RemoveAll("tables")
		}()

		// Создаем директорию
		err := os.MkdirAll(filepath.Dir(TABLE_LIST_FILE_PATH), 0755)
		require.NoError(t, err)

		tablesList := NewTablesList()
		tablesList.Tables["test_table"] = FileID{FileID: 1}

		// Act
		writtenList, err := writeTablesListFile(tablesList)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, writtenList)
		require.Equal(t, tablesList.Header.MagicNumber, writtenList.Header.MagicNumber)
		require.Equal(t, len(tablesList.Tables), len(writtenList.Tables))

		// Проверяем, что файл записан
		_, err = os.Stat(TABLE_LIST_FILE_PATH)
		require.NoError(t, err)

		// Проверяем, что данные записались корректно
		readList, err := readTableListFile()
		require.NoError(t, err)
		require.Equal(t, uint32(1), readList.Tables["test_table"].FileID)

		// Cleanup
		os.RemoveAll("tables")
	})
}

func TestAddTableInList(t *testing.T) {
	t.Run("1. Add table to new list", func(t *testing.T) {
		// Arrange
		defer func() {
			os.RemoveAll("tables")
		}()

		// Создаем директорию для тестов
		err := os.MkdirAll(filepath.Dir(TABLE_LIST_FILE_PATH), 0755)
		require.NoError(t, err)

		// Создаем файл списка таблиц
		_, err = createTableListFile()
		require.NoError(t, err)

		tableName := "new_table"

		// Act
		tablesList, err := addTableInList(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, tablesList)
		require.Equal(t, 1, len(tablesList.Tables))
		require.Contains(t, tablesList.Tables, tableName)
		require.Equal(t, uint32(1), tablesList.Tables[tableName].FileID)

		// Проверяем, что файл создан
		_, err = os.Stat(TABLE_LIST_FILE_PATH)
		require.NoError(t, err)

		// Cleanup
		os.RemoveAll("tables")
	})

	t.Run("2. Add table to existing list", func(t *testing.T) {
		// Arrange
		defer func() {
			os.RemoveAll("tables")
		}()

		// Создаем директорию для тестов
		err := os.MkdirAll(filepath.Dir(TABLE_LIST_FILE_PATH), 0755)
		require.NoError(t, err)

		// Создаем файл списка таблиц
		_, err = createTableListFile()
		require.NoError(t, err)

		// Создаем первую таблицу
		firstTable := "first_table"
		_, err = addTableInList(firstTable)
		require.NoError(t, err)

		// Act - добавляем вторую таблицу
		secondTable := "second_table"
		tablesList, err := addTableInList(secondTable)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, tablesList)
		require.Equal(t, 2, len(tablesList.Tables))
		require.Contains(t, tablesList.Tables, firstTable)
		require.Contains(t, tablesList.Tables, secondTable)
		require.Equal(t, uint32(1), tablesList.Tables[firstTable].FileID)
		require.Equal(t, uint32(2), tablesList.Tables[secondTable].FileID)

		// Cleanup
		os.RemoveAll("tables")
	})

	t.Run("3. Add table with name too long", func(t *testing.T) {
		// Arrange
		defer func() {
			os.RemoveAll("tables")
		}()

		// Создаем директорию для тестов
		err := os.MkdirAll(filepath.Dir(TABLE_LIST_FILE_PATH), 0755)
		require.NoError(t, err)

		// Создаем файл списка таблиц
		_, err = createTableListFile()
		require.NoError(t, err)

		longTableName := strings.Repeat("a", TABLE_NAME_MAX_LENGTH+1)

		// Act
		tablesList, err := addTableInList(longTableName)

		// Assert
		require.Error(t, err)
		require.Nil(t, tablesList)
		require.Contains(t, err.Error(), "table name too long")

		// Cleanup
		os.RemoveAll("tables")
	})

	t.Run("4. Add table when file does not exist", func(t *testing.T) {
		// Act
		tablesList, err := addTableInList("some_table")

		// Assert
		require.Error(t, err)
		require.Nil(t, tablesList)
		require.Contains(t, err.Error(), "failed to read tables list")

		// Cleanup
		os.RemoveAll("tables")
	})
}

func TestDeleteTableInList(t *testing.T) {
	t.Run("1. Delete table from list success", func(t *testing.T) {
		// Arrange
		defer func() {
			os.RemoveAll("tables")
		}()

		// Создаем директорию для тестов
		err := os.MkdirAll(filepath.Dir(TABLE_LIST_FILE_PATH), 0755)
		require.NoError(t, err)

		// Создаем файл списка таблиц
		_, err = createTableListFile()
		require.NoError(t, err)

		// Создаем две таблицы
		table1 := "table_one"
		table2 := "table_two"
		_, err = addTableInList(table1)
		require.NoError(t, err)
		_, err = addTableInList(table2)
		require.NoError(t, err)

		// Act - удаляем одну таблицу
		tablesList, err := deleteTableInList(table1)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, tablesList)
		require.Equal(t, 1, len(tablesList.Tables))
		require.NotContains(t, tablesList.Tables, table1)
		require.Contains(t, tablesList.Tables, table2)

		// Cleanup
		os.RemoveAll("tables")
	})

	t.Run("2. Delete table from empty list", func(t *testing.T) {
		// Arrange
		defer func() {
			os.RemoveAll("tables")
		}()

		// Создаем пустой список
		_, err := createTableListFile()
		require.NoError(t, err)

		// Act - пытаемся удалить несуществующую таблицу
		tablesList, err := deleteTableInList("nonexistent_table")

		// Assert
		require.NoError(t, err)
		require.NotNil(t, tablesList)
		require.Equal(t, 0, len(tablesList.Tables))

		// Cleanup
		os.RemoveAll("tables")
	})

	t.Run("3. Delete table when file does not exist", func(t *testing.T) {
		// Act
		tablesList, err := deleteTableInList("some_table")

		// Assert
		require.Error(t, err)
		require.Nil(t, tablesList)
		require.Contains(t, err.Error(), "failed to read tables list")

		// Cleanup
		os.RemoveAll("tables")
	})
}
