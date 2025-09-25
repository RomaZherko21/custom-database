package disk_manager

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSerializeMetaDataHeader(t *testing.T) {
	t.Run("1. Meta file header serialization success", func(t *testing.T) {
		// Arrange
		tableName := "users"
		columnCount := uint32(3)
		header := newMetaFileHeader(tableName, columnCount)

		// Act
		data := header.Serialize()

		// Assert
		require.Len(t, data, META_FILE_HEADER_SIZE)
		require.Equal(t, uint32(META_FILE_MAGIC_NUMBER), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(len(tableName)), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, columnCount, binary.BigEndian.Uint32(data[8:12]))
		require.Equal(t, tableName, string(data[12:12+len(tableName)]))
		require.Equal(t, uint64(0), binary.BigEndian.Uint64(data[44:52]))
	})

	t.Run("2. Meta file header serialization with long table name", func(t *testing.T) {
		// Arrange
		tableName := "very_long_table_name_that_exceeds_normal_length"
		columnCount := uint32(5)

		// Проверяем, что имя действительно длиннее максимального
		require.Greater(t, len(tableName), TABLE_NAME_MAX_LENGTH)

		header := newMetaFileHeader(tableName, columnCount)

		// Act
		data := header.Serialize()

		// Assert
		require.Len(t, data, META_FILE_HEADER_SIZE)
		require.Equal(t, uint32(META_FILE_MAGIC_NUMBER), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(TABLE_NAME_MAX_LENGTH), binary.BigEndian.Uint32(data[4:8])) // Обрезано до максимальной длины
		require.Equal(t, columnCount, binary.BigEndian.Uint32(data[8:12]))
		// Проверяем, что имя таблицы обрезано до максимальной длины
		expectedName := tableName[:TABLE_NAME_MAX_LENGTH]
		require.Equal(t, expectedName, string(data[12:12+TABLE_NAME_MAX_LENGTH]))
	})

	t.Run("3. Meta file header serialization with zero columns", func(t *testing.T) {
		// Arrange
		tableName := "empty_table"
		columnCount := uint32(0)
		header := newMetaFileHeader(tableName, columnCount)

		// Act
		data := header.Serialize()

		// Assert
		require.Len(t, data, META_FILE_HEADER_SIZE)
		require.Equal(t, uint32(META_FILE_MAGIC_NUMBER), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(len(tableName)), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, columnCount, binary.BigEndian.Uint32(data[8:12]))
		require.Equal(t, tableName, string(data[12:12+len(tableName)]))
	})
}

func TestDeserializeMetaDataHeader(t *testing.T) {
	t.Run("1. Meta file header deserialization success", func(t *testing.T) {
		// Arrange
		tableName := "users"
		columnCount := uint32(3)
		originalHeader := newMetaFileHeader(tableName, columnCount)
		data := originalHeader.Serialize()

		// Act
		deserializedHeader, err := (&MetaDataHeader{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.Equal(t, originalHeader.MagicNumber, deserializedHeader.MagicNumber)
		require.Equal(t, originalHeader.TableNameLen, deserializedHeader.TableNameLen)
		require.Equal(t, originalHeader.ColumnCount, deserializedHeader.ColumnCount)
		require.Equal(t, originalHeader.NextRowID, deserializedHeader.NextRowID)
		require.Equal(t, originalHeader.TableName, deserializedHeader.TableName)
	})

	t.Run("2. Meta file header deserialization with insufficient data", func(t *testing.T) {
		// Arrange
		insufficientData := make([]byte, META_FILE_HEADER_SIZE-1)

		// Act
		header, err := (&MetaDataHeader{}).Deserialize(insufficientData)

		// Assert
		require.Error(t, err)
		require.Nil(t, header)
		require.Contains(t, err.Error(), "insufficient data for meta file header")
	})

	t.Run("3. Meta file header deserialization with empty data", func(t *testing.T) {
		// Arrange
		emptyData := make([]byte, 0)

		// Act
		header, err := (&MetaDataHeader{}).Deserialize(emptyData)

		// Assert
		require.Error(t, err)
		require.Nil(t, header)
		require.Contains(t, err.Error(), "insufficient data for meta file header")
	})
}

func TestSerializeColumnInfo(t *testing.T) {
	t.Run("1. Column info serialization success", func(t *testing.T) {
		// Arrange
		column := ColumnInfo{
			ColumnNameLength: 2,
			DataType:         INT_32_TYPE,
			IsNullable:       1,
			IsPrimaryKey:     1,
			IsAutoIncrement:  0,
			DefaultValue:     0,
			ColumnName:       "id",
		}

		// Act
		data := column.Serialize()

		// Assert
		require.Len(t, data, COLUMN_INFO_SIZE)
		require.Equal(t, uint32(2), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(INT_32_TYPE), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(1), binary.BigEndian.Uint32(data[8:12]))
		require.Equal(t, uint32(1), binary.BigEndian.Uint32(data[12:16]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[16:20]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[20:24]))
		require.Equal(t, "id", string(data[24:26]))
	})

	t.Run("2. Column info serialization with TEXT type", func(t *testing.T) {
		// Arrange
		column := ColumnInfo{
			ColumnNameLength: 4,
			DataType:         TEXT_TYPE,
			IsNullable:       0,
			IsPrimaryKey:     0,
			IsAutoIncrement:  0,
			DefaultValue:     0,
			ColumnName:       "name",
		}

		// Act
		data := column.Serialize()

		// Assert
		require.Len(t, data, COLUMN_INFO_SIZE)
		require.Equal(t, uint32(4), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(TEXT_TYPE), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[8:12]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[12:16]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[16:20]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[20:24]))
		require.Equal(t, "name", string(data[24:28]))
	})

	t.Run("3. Column info serialization with long name", func(t *testing.T) {
		// Arrange
		longName := "very_long_column_name_that_exceeds_normal_length"
		// Обрезаем до максимальной длины
		truncatedName := longName[:COLUMN_NAME_MAX_LENGTH]
		column := ColumnInfo{
			ColumnNameLength: uint32(len(truncatedName)),
			DataType:         INT_32_TYPE,
			IsNullable:       1,
			IsPrimaryKey:     0,
			IsAutoIncrement:  1,
			DefaultValue:     100,
			ColumnName:       truncatedName,
		}

		// Act
		data := column.Serialize()

		// Assert
		require.Len(t, data, COLUMN_INFO_SIZE)
		require.Equal(t, uint32(len(truncatedName)), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(INT_32_TYPE), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(1), binary.BigEndian.Uint32(data[8:12]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[12:16]))
		require.Equal(t, uint32(1), binary.BigEndian.Uint32(data[16:20]))
		require.Equal(t, uint32(100), binary.BigEndian.Uint32(data[20:24]))
		// Проверяем, что не выходим за границы массива
		readLength := len(truncatedName)
		if 24+readLength > COLUMN_INFO_SIZE {
			readLength = COLUMN_INFO_SIZE - 24
		}
		// Ожидаем обрезанное имя
		expectedName := truncatedName[:readLength]
		require.Equal(t, expectedName, string(data[24:24+readLength]))
	})
}

func TestDeserializeColumnInfo(t *testing.T) {
	t.Run("1. Column info deserialization success", func(t *testing.T) {
		// Arrange
		originalColumn := ColumnInfo{
			ColumnNameLength: 2,
			DataType:         INT_32_TYPE,
			IsNullable:       1,
			IsPrimaryKey:     1,
			IsAutoIncrement:  0,
			DefaultValue:     0,
			ColumnName:       "id",
		}
		data := originalColumn.Serialize()

		// Act
		deserializedColumn, err := (&ColumnInfo{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.Equal(t, originalColumn.ColumnNameLength, deserializedColumn.ColumnNameLength)
		require.Equal(t, originalColumn.DataType, deserializedColumn.DataType)
		require.Equal(t, originalColumn.IsNullable, deserializedColumn.IsNullable)
		require.Equal(t, originalColumn.IsPrimaryKey, deserializedColumn.IsPrimaryKey)
		require.Equal(t, originalColumn.IsAutoIncrement, deserializedColumn.IsAutoIncrement)
		require.Equal(t, originalColumn.DefaultValue, deserializedColumn.DefaultValue)
		require.Equal(t, originalColumn.ColumnName, deserializedColumn.ColumnName)
	})

	t.Run("2. Column info deserialization with insufficient data", func(t *testing.T) {
		// Arrange
		insufficientData := make([]byte, COLUMN_INFO_SIZE-1)

		// Act
		column, err := (&ColumnInfo{}).Deserialize(insufficientData)

		// Assert
		require.Error(t, err)
		require.Nil(t, column)
		require.Contains(t, err.Error(), "insufficient data for column info")
	})

	t.Run("3. Column info deserialization with empty data", func(t *testing.T) {
		// Arrange
		emptyData := make([]byte, 0)

		// Act
		column, err := (&ColumnInfo{}).Deserialize(emptyData)

		// Assert
		require.Error(t, err)
		require.Nil(t, column)
		require.Contains(t, err.Error(), "insufficient data for column info")
	})
}

func TestCreateMetaFile(t *testing.T) {
	t.Run("1. Create meta file success", func(t *testing.T) {
		// Arrange
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

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Act
		metaData, err := createMetaFile(tableName, columns)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, metaData)
		require.Equal(t, tableName, metaData.Header.TableName)
		require.Equal(t, uint32(len(columns)), metaData.Header.ColumnCount)
		require.Len(t, metaData.Columns, len(columns))

		// Проверяем каждую колонку детально
		// Первая колонка (id)
		require.Equal(t, uint32(2), metaData.Columns[0].ColumnNameLength)
		require.Equal(t, INT_32_TYPE, metaData.Columns[0].DataType)
		require.Equal(t, uint32(0), metaData.Columns[0].IsNullable)
		require.Equal(t, uint32(1), metaData.Columns[0].IsPrimaryKey)
		require.Equal(t, uint32(1), metaData.Columns[0].IsAutoIncrement)
		require.Equal(t, uint32(0), metaData.Columns[0].DefaultValue)
		require.Equal(t, "id", metaData.Columns[0].ColumnName)

		// Вторая колонка (name)
		require.Equal(t, uint32(4), metaData.Columns[1].ColumnNameLength)
		require.Equal(t, TEXT_TYPE, metaData.Columns[1].DataType)
		require.Equal(t, uint32(1), metaData.Columns[1].IsNullable)
		require.Equal(t, uint32(0), metaData.Columns[1].IsPrimaryKey)
		require.Equal(t, uint32(0), metaData.Columns[1].IsAutoIncrement)
		require.Equal(t, uint32(0), metaData.Columns[1].DefaultValue)
		require.Equal(t, "name", metaData.Columns[1].ColumnName)

		// Проверяем размеры сериализованных данных
		headerData := metaData.Header.Serialize()
		require.Len(t, headerData, META_FILE_HEADER_SIZE)

		// Проверяем размеры каждой колонки
		for i, column := range metaData.Columns {
			columnData := column.Serialize()
			require.Len(t, columnData, COLUMN_INFO_SIZE, "Column %d should be %d bytes", i, COLUMN_INFO_SIZE)
		}

		// Проверяем, что файл создался
		metaFilePath := filepath.Join("tables", tableName+".meta")
		_, err = os.Stat(metaFilePath)
		require.NoError(t, err)

		// Cleanup
		// err = os.Remove(metaFilePath)
		require.NoError(t, err)
	})

	t.Run("2. Create meta file when table already exists", func(t *testing.T) {
		// Arrange
		tableName := "existing_table"
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
		}

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем файл заранее
		metaFilePath := filepath.Join("tables", tableName+".meta")
		file, err := os.Create(metaFilePath)
		require.NoError(t, err)
		file.Close()

		// Act
		metaData, err := createMetaFile(tableName, columns)

		// Assert
		require.Error(t, err)
		require.Nil(t, metaData)
		require.Contains(t, err.Error(), "table existing_table already exists")

		// Cleanup
		err = os.Remove(metaFilePath)
		require.NoError(t, err)
	})

	t.Run("3. Create meta file with empty columns", func(t *testing.T) {
		// Arrange
		tableName := "empty_table"
		columns := []ColumnInfo{}

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Act
		metaData, err := createMetaFile(tableName, columns)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, metaData)
		require.Equal(t, tableName, metaData.Header.TableName)
		require.Equal(t, uint32(0), metaData.Header.ColumnCount)
		require.Len(t, metaData.Columns, 0)

		// Cleanup
		metaFilePath := filepath.Join("tables", tableName+".meta")
		err = os.Remove(metaFilePath)
		require.NoError(t, err)
	})
}

func TestReadMetaFile(t *testing.T) {
	t.Run("1. Read meta file success", func(t *testing.T) {
		// Arrange
		tableName := "read_test_table" // 15
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

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем meta файл
		createdMeta, err := createMetaFile(tableName, columns)
		require.NoError(t, err)

		// Act
		readMeta, err := readMetaFile(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, readMeta)
		require.Equal(t, createdMeta.Header.MagicNumber, readMeta.Header.MagicNumber)
		require.Equal(t, createdMeta.Header.TableNameLen, readMeta.Header.TableNameLen)
		require.Equal(t, createdMeta.Header.ColumnCount, readMeta.Header.ColumnCount)
		require.Equal(t, createdMeta.Header.TableName, readMeta.Header.TableName)
		require.Equal(t, createdMeta.Header.NextRowID, readMeta.Header.NextRowID)
		require.Len(t, readMeta.Columns, len(columns))

		// Cleanup
		// metaFilePath := filepath.Join("tables", tableName+".meta")
		// err = os.Remove(metaFilePath)
		require.NoError(t, err)
	})

	t.Run("2. Read meta file when table does not exist", func(t *testing.T) {
		// Arrange
		tableName := "non_existent_table"

		// Act
		metaData, err := readMetaFile(tableName)

		// Assert
		require.Error(t, err)
		require.Nil(t, metaData)
		require.Contains(t, err.Error(), "table non_existent_table not found")
	})

	t.Run("3. Read meta file with invalid magic number", func(t *testing.T) {
		// Arrange
		tableName := "invalid_magic_table"
		metaFilePath := filepath.Join("tables", tableName+".meta")

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем файл с неправильным magic number
		file, err := os.Create(metaFilePath)
		require.NoError(t, err)
		defer file.Close()

		// Записываем неправильный magic number и заполняем остальные байты заголовка
		wrongMagic := make([]byte, META_FILE_HEADER_SIZE)
		binary.BigEndian.PutUint32(wrongMagic, 0x12345678) // Неправильный magic number
		_, err = file.Write(wrongMagic)
		require.NoError(t, err)

		// Act
		metaData, err := readMetaFile(tableName)

		// Assert
		require.Error(t, err)
		require.Nil(t, metaData)
		require.Contains(t, err.Error(), "invalid magic number")

		// Cleanup
		err = os.Remove(metaFilePath)
		require.NoError(t, err)
	})
}

func TestWriteMetaFile(t *testing.T) {
	t.Run("1. Write meta file success", func(t *testing.T) {
		// Arrange
		tableName := "write_test_table"
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
		}

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем исходный meta файл
		originalMeta, err := createMetaFile(tableName, columns)
		require.NoError(t, err)

		// Изменяем NextRowID
		originalMeta.Header.NextRowID = 100

		// Act
		writtenMeta, err := writeMetaFile(tableName, originalMeta)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, writtenMeta)
		require.Equal(t, uint64(100), writtenMeta.Header.NextRowID)

		// Проверяем, что изменения записались
		readMeta, err := readMetaFile(tableName)
		require.NoError(t, err)
		require.Equal(t, uint64(100), readMeta.Header.NextRowID)

		// Cleanup
		metaFilePath := filepath.Join("tables", tableName+".meta")
		err = os.Remove(metaFilePath)
		require.NoError(t, err)
	})

	t.Run("2. Write meta file when table does not exist", func(t *testing.T) {
		// Arrange
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
		writtenMeta, err := writeMetaFile(tableName, metaData)

		// Assert
		require.Error(t, err)
		require.Nil(t, writtenMeta)
		require.Contains(t, err.Error(), "table non_existent_write_table not found")
	})
}

func TestDeleteMetaFile(t *testing.T) {
	t.Run("1. Delete meta file success", func(t *testing.T) {
		// Arrange
		tableName := "delete_test_table"
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
		}

		// Создаем папку tables если не существует
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Создаем meta файл
		_, err = createMetaFile(tableName, columns)
		require.NoError(t, err)

		// Проверяем, что файл существует
		metaFilePath := filepath.Join("tables", tableName+".meta")
		_, err = os.Stat(metaFilePath)
		require.NoError(t, err)

		// Act
		err = deleteMetaFile(tableName)

		// Assert
		require.NoError(t, err)

		// Проверяем, что файл удален
		_, err = os.Stat(metaFilePath)
		require.Error(t, err)
		require.True(t, os.IsNotExist(err))
	})

	t.Run("2. Delete meta file when table does not exist", func(t *testing.T) {
		// Arrange
		tableName := "non_existent_delete_table"

		// Act
		err := deleteMetaFile(tableName)

		// Assert
		require.Error(t, err)
		require.Contains(t, err.Error(), "table non_existent_delete_table not found")
	})
}
