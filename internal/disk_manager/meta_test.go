package disk_manager

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetaFileHeader_Serialize(t *testing.T) {
	t.Run("should serialize meta file header correctly", func(t *testing.T) {
		// Arrange
		meta := NewMetaFileHeader("users", 3)
		meta.NextRowID = 42

		// Act
		result := meta.Serialize()

		// Assert
		require.NotNil(t, result)
		assert.Equal(t, 56, len(result), "Serialized data should be 56 bytes")

		// Проверяем MagicNumber (первые 4 байта)
		assert.Equal(t, uint32(0x9ABCDEF0), binary.BigEndian.Uint32(result[0:4]))

		// Проверяем TableNameLen (байты 4-8)
		assert.Equal(t, uint32(5), binary.BigEndian.Uint32(result[4:8]))

		// Проверяем ColumnCount (байты 8-12)
		assert.Equal(t, uint32(3), binary.BigEndian.Uint32(result[8:12]))

		// Проверяем TableName (байты 12-44)
		expectedTableName := [32]byte{'u', 's', 'e', 'r', 's', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		assert.Equal(t, expectedTableName, [32]byte(result[12:44]))

		// Проверяем NextRowID (байты 44-52)
		assert.Equal(t, uint64(42), binary.BigEndian.Uint64(result[44:52]))
	})

	t.Run("should serialize with zero values", func(t *testing.T) {
		// Arrange
		meta := NewMetaFileHeader("", 0)

		// Act
		result := meta.Serialize()

		// Assert
		require.NotNil(t, result)
		assert.Equal(t, 56, len(result))

		// Проверяем MagicNumber
		assert.Equal(t, uint32(0x9ABCDEF0), binary.BigEndian.Uint32(result[0:4]))

		// Проверяем TableNameLen
		assert.Equal(t, uint32(0), binary.BigEndian.Uint32(result[4:8]))

		// Проверяем ColumnCount
		assert.Equal(t, uint32(0), binary.BigEndian.Uint32(result[8:12]))

		// Проверяем TableName
		assert.Equal(t, [32]byte{}, [32]byte(result[12:44]))

		// Проверяем NextRowID
		assert.Equal(t, uint64(0), binary.BigEndian.Uint64(result[44:52]))
	})

	t.Run("should serialize with maximum values", func(t *testing.T) {
		// Arrange
		meta := NewMetaFileHeader("very_long_table_name_exceeding_32_chars", 0xFFFFFFFF)
		meta.NextRowID = 0xFFFFFFFFFFFFFFFF

		// Act
		result := meta.Serialize()

		// Assert
		require.NotNil(t, result)
		assert.Equal(t, 56, len(result))

		// Проверяем максимальные значения
		assert.Equal(t, uint32(0x9ABCDEF0), binary.BigEndian.Uint32(result[0:4]))
		assert.Equal(t, uint32(39), binary.BigEndian.Uint32(result[4:8])) // Длина исходной строки
		assert.Equal(t, uint32(0xFFFFFFFF), binary.BigEndian.Uint32(result[8:12]))
		assert.Equal(t, uint64(0xFFFFFFFFFFFFFFFF), binary.BigEndian.Uint64(result[44:52]))
	})
}

func TestMetaFileHeader_Deserialize(t *testing.T) {
	t.Run("should deserialize meta file header correctly", func(t *testing.T) {
		// Arrange
		data := make([]byte, 56)
		binary.BigEndian.PutUint32(data[0:4], 0x9ABCDEF0)
		binary.BigEndian.PutUint32(data[4:8], 5)
		binary.BigEndian.PutUint32(data[8:12], 3)
		copy(data[12:44], []byte{'u', 's', 'e', 'r', 's', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
		binary.BigEndian.PutUint64(data[44:52], 42)

		// Act
		result, err := (&MetaFileHeader{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, uint32(0x9ABCDEF0), result.MagicNumber)
		assert.Equal(t, uint32(5), result.TableNameLen)
		assert.Equal(t, uint32(3), result.ColumnCount)
		assert.Equal(t, [32]byte{'u', 's', 'e', 'r', 's', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, result.TableName)
		assert.Equal(t, uint64(42), result.NextRowID)
	})

	t.Run("should deserialize with zero values", func(t *testing.T) {
		// Arrange
		data := make([]byte, 56)
		// Все байты уже равны 0

		// Act
		result, err := (&MetaFileHeader{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, uint32(0), result.MagicNumber)
		assert.Equal(t, uint32(0), result.TableNameLen)
		assert.Equal(t, uint32(0), result.ColumnCount)
		assert.Equal(t, [32]byte{}, result.TableName)
		assert.Equal(t, uint64(0), result.NextRowID)
	})

	t.Run("should deserialize with maximum values", func(t *testing.T) {
		// Arrange
		data := make([]byte, 56)
		binary.BigEndian.PutUint32(data[0:4], 0xFFFFFFFF)
		binary.BigEndian.PutUint32(data[4:8], 0xFFFFFFFF)
		binary.BigEndian.PutUint32(data[8:12], 0xFFFFFFFF)
		for i := 12; i < 44; i++ {
			data[i] = 0xFF
		}
		binary.BigEndian.PutUint64(data[44:52], 0xFFFFFFFFFFFFFFFF)

		// Act
		result, err := (&MetaFileHeader{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, uint32(0xFFFFFFFF), result.MagicNumber)
		assert.Equal(t, uint32(0xFFFFFFFF), result.TableNameLen)
		assert.Equal(t, uint32(0xFFFFFFFF), result.ColumnCount)
		assert.Equal(t, [32]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, result.TableName)
		assert.Equal(t, uint64(0xFFFFFFFFFFFFFFFF), result.NextRowID)
	})

	t.Run("should return error for insufficient data", func(t *testing.T) {
		// Arrange
		data := make([]byte, 55) // Меньше 56 байт

		// Act
		result, err := (&MetaFileHeader{}).Deserialize(data)

		// Assert
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "insufficient data for meta file header")
	})

	t.Run("should return error for empty data", func(t *testing.T) {
		// Arrange
		data := make([]byte, 0)

		// Act
		result, err := (&MetaFileHeader{}).Deserialize(data)

		// Assert
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "insufficient data for meta file header")
	})

	t.Run("should return error for nil data", func(t *testing.T) {
		// Arrange
		var data []byte = nil

		// Act
		result, err := (&MetaFileHeader{}).Deserialize(data)

		// Assert
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "insufficient data for meta file header")
	})
}

func TestMetaFileHeader_SerializeDeserialize_RoundTrip(t *testing.T) {
	t.Run("should maintain data integrity through serialize-deserialize", func(t *testing.T) {
		// Arrange
		original := NewMetaFileHeader("users", 3)
		original.NextRowID = 42

		// Act
		serialized := original.Serialize()
		deserialized, err := (&MetaFileHeader{}).Deserialize(serialized)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, deserialized)

		assert.Equal(t, original.MagicNumber, deserialized.MagicNumber)
		assert.Equal(t, original.TableNameLen, deserialized.TableNameLen)
		assert.Equal(t, original.ColumnCount, deserialized.ColumnCount)
		assert.Equal(t, original.TableName, deserialized.TableName)
		assert.Equal(t, original.NextRowID, deserialized.NextRowID)
	})

	t.Run("should maintain data integrity with zero values", func(t *testing.T) {
		// Arrange
		original := NewMetaFileHeader("", 0)

		// Act
		serialized := original.Serialize()
		deserialized, err := (&MetaFileHeader{}).Deserialize(serialized)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, deserialized)

		assert.Equal(t, original.MagicNumber, deserialized.MagicNumber)
		assert.Equal(t, original.TableNameLen, deserialized.TableNameLen)
		assert.Equal(t, original.ColumnCount, deserialized.ColumnCount)
		assert.Equal(t, original.TableName, deserialized.TableName)
		assert.Equal(t, original.NextRowID, deserialized.NextRowID)
	})

	t.Run("should maintain data integrity with maximum values", func(t *testing.T) {
		// Arrange
		original := NewMetaFileHeader("very_long_table_name_exceeding_32_chars", 0xFFFFFFFF)
		original.NextRowID = 0xFFFFFFFFFFFFFFFF

		// Act
		serialized := original.Serialize()
		deserialized, err := (&MetaFileHeader{}).Deserialize(serialized)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, deserialized)

		assert.Equal(t, original.MagicNumber, deserialized.MagicNumber)
		assert.Equal(t, original.TableNameLen, deserialized.TableNameLen)
		assert.Equal(t, original.ColumnCount, deserialized.ColumnCount)
		assert.Equal(t, original.TableName, deserialized.TableName)
		assert.Equal(t, original.NextRowID, deserialized.NextRowID)
	})
}

func TestColumnInfo_Serialize(t *testing.T) {
	t.Run("should serialize column info correctly", func(t *testing.T) {
		// Arrange
		column := NewColumnInfo("id", uint32(INT_32_TYPE), 0, 1, 1, 0)

		// Act
		result := column.Serialize()

		// Assert
		require.NotNil(t, result)
		assert.Equal(t, 52, len(result), "Serialized data should be 52 bytes")

		// Проверяем ColumnName (байты 0-32)
		expectedColumnName := [32]byte{'i', 'd', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		assert.Equal(t, expectedColumnName, [32]byte(result[0:32]))

		// Проверяем DataType (байты 32-36)
		assert.Equal(t, uint32(INT_32_TYPE), binary.BigEndian.Uint32(result[32:36]))

		// Проверяем IsNullable (байты 36-40)
		assert.Equal(t, uint32(0), binary.BigEndian.Uint32(result[36:40]))

		// Проверяем IsPrimaryKey (байты 40-44)
		assert.Equal(t, uint32(1), binary.BigEndian.Uint32(result[40:44]))

		// Проверяем IsAutoIncrement (байты 44-48)
		assert.Equal(t, uint32(1), binary.BigEndian.Uint32(result[44:48]))

		// Проверяем DefaultValue (байты 48-52)
		assert.Equal(t, uint32(0), binary.BigEndian.Uint32(result[48:52]))
	})

	t.Run("should serialize with zero values", func(t *testing.T) {
		// Arrange
		column := NewColumnInfo("", uint32(INT_32_TYPE), 0, 0, 0, 0)

		// Act
		result := column.Serialize()

		// Assert
		require.NotNil(t, result)
		assert.Equal(t, 52, len(result))

		// Проверяем, что все поля равны нулю (кроме DataType)
		assert.Equal(t, [32]byte{}, [32]byte(result[0:32]))
		assert.Equal(t, uint32(INT_32_TYPE), binary.BigEndian.Uint32(result[32:36]))
		assert.Equal(t, uint32(0), binary.BigEndian.Uint32(result[36:40]))
		assert.Equal(t, uint32(0), binary.BigEndian.Uint32(result[40:44]))
		assert.Equal(t, uint32(0), binary.BigEndian.Uint32(result[44:48]))
		assert.Equal(t, uint32(0), binary.BigEndian.Uint32(result[48:52]))
	})
}

func TestColumnInfo_Deserialize(t *testing.T) {
	t.Run("should deserialize column info correctly", func(t *testing.T) {
		// Arrange
		data := make([]byte, 52)
		copy(data[0:32], []byte{'i', 'd', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
		binary.BigEndian.PutUint32(data[32:36], uint32(INT_32_TYPE))
		binary.BigEndian.PutUint32(data[36:40], 0)
		binary.BigEndian.PutUint32(data[40:44], 1)
		binary.BigEndian.PutUint32(data[44:48], 1)
		binary.BigEndian.PutUint32(data[48:52], 0)

		// Act
		result, err := (&ColumnInfo{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, result)

		expectedColumnName := [32]byte{'i', 'd', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		assert.Equal(t, expectedColumnName, result.ColumnName)
		assert.Equal(t, INT_32_TYPE, result.DataType)
		assert.Equal(t, uint32(0), result.IsNullable)
		assert.Equal(t, uint32(1), result.IsPrimaryKey)
		assert.Equal(t, uint32(1), result.IsAutoIncrement)
		assert.Equal(t, uint32(0), result.DefaultValue)
	})

	t.Run("should deserialize with zero values", func(t *testing.T) {
		// Arrange
		data := make([]byte, 52)
		// Все байты уже равны 0, что означает DataType = 0 (не INT_32_TYPE)

		// Act
		result, err := (&ColumnInfo{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, [32]byte{}, result.ColumnName)
		assert.Equal(t, DataType(0), result.DataType) // 0 = неопределенный тип
		assert.Equal(t, uint32(0), result.IsNullable)
		assert.Equal(t, uint32(0), result.IsPrimaryKey)
		assert.Equal(t, uint32(0), result.IsAutoIncrement)
		assert.Equal(t, uint32(0), result.DefaultValue)
	})

	t.Run("should return error for insufficient data", func(t *testing.T) {
		// Arrange
		data := make([]byte, 51) // Меньше 52 байт

		// Act
		result, err := (&ColumnInfo{}).Deserialize(data)

		// Assert
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "insufficient data for column info")
	})

	t.Run("should return error for empty data", func(t *testing.T) {
		// Arrange
		data := make([]byte, 0)

		// Act
		result, err := (&ColumnInfo{}).Deserialize(data)

		// Assert
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "insufficient data for column info")
	})

	t.Run("should return error for nil data", func(t *testing.T) {
		// Arrange
		var data []byte = nil

		// Act
		result, err := (&ColumnInfo{}).Deserialize(data)

		// Assert
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "insufficient data for column info")
	})
}

func TestColumnInfo_SerializeDeserialize_RoundTrip(t *testing.T) {
	t.Run("should maintain data integrity through serialize-deserialize", func(t *testing.T) {
		// Arrange
		original := NewColumnInfo("name", uint32(TEXT_TYPE), 1, 0, 0, 42)

		// Act
		serialized := original.Serialize()
		deserialized, err := (&ColumnInfo{}).Deserialize(serialized)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, deserialized)

		assert.Equal(t, original.ColumnName, deserialized.ColumnName)
		assert.Equal(t, original.DataType, deserialized.DataType)
		assert.Equal(t, original.IsNullable, deserialized.IsNullable)
		assert.Equal(t, original.IsPrimaryKey, deserialized.IsPrimaryKey)
		assert.Equal(t, original.IsAutoIncrement, deserialized.IsAutoIncrement)
		assert.Equal(t, original.DefaultValue, deserialized.DefaultValue)
	})

	t.Run("should maintain data integrity with zero values", func(t *testing.T) {
		// Arrange
		original := NewColumnInfo("", uint32(INT_32_TYPE), 0, 0, 0, 0)

		// Act
		serialized := original.Serialize()
		deserialized, err := (&ColumnInfo{}).Deserialize(serialized)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, deserialized)

		assert.Equal(t, original.ColumnName, deserialized.ColumnName)
		assert.Equal(t, original.DataType, deserialized.DataType)
		assert.Equal(t, original.IsNullable, deserialized.IsNullable)
		assert.Equal(t, original.IsPrimaryKey, deserialized.IsPrimaryKey)
		assert.Equal(t, original.IsAutoIncrement, deserialized.IsAutoIncrement)
		assert.Equal(t, original.DefaultValue, deserialized.DefaultValue)
	})
}

func TestCreateMetaFile(t *testing.T) {
	t.Run("should create meta file with single column", func(t *testing.T) {
		// Arrange
		tableName := "test_users"
		columns := []ColumnInfo{
			*NewColumnInfo("id", uint32(INT_32_TYPE), 0, 1, 1, 0),
		}
		metaFilePath := filepath.Join("tables", tableName+".meta")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(metaFilePath)
			os.Remove("tables")
		}()

		// Act
		metaFile, err := CreateMetaFile(tableName, columns)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, metaFile)
		require.NotNil(t, metaFile.Header)
		require.Len(t, metaFile.Columns, 1)

		// Check file exists
		_, err = os.Stat(metaFilePath)
		require.NoError(t, err, "meta file should exist")

		// Check header
		assert.Equal(t, uint32(META_FILE_MAGIC_NUMBER), metaFile.Header.MagicNumber)
		assert.Equal(t, uint32(len(tableName)), metaFile.Header.TableNameLen)
		assert.Equal(t, uint32(1), metaFile.Header.ColumnCount)
		assert.Equal(t, uint64(0), metaFile.Header.NextRowID)

		// Check column
		assert.Equal(t, "id", string(metaFile.Columns[0].ColumnName[:2]))
		assert.Equal(t, INT_32_TYPE, metaFile.Columns[0].DataType)
		assert.Equal(t, uint32(0), metaFile.Columns[0].IsNullable)
		assert.Equal(t, uint32(1), metaFile.Columns[0].IsPrimaryKey)
		assert.Equal(t, uint32(1), metaFile.Columns[0].IsAutoIncrement)
		assert.Equal(t, uint32(0), metaFile.Columns[0].DefaultValue)
	})

	t.Run("should create meta file with multiple columns", func(t *testing.T) {
		// Arrange
		tableName := "test_products"
		columns := []ColumnInfo{
			*NewColumnInfo("id", uint32(INT_32_TYPE), 0, 1, 1, 0),
			*NewColumnInfo("name", uint32(TEXT_TYPE), 1, 0, 0, 0),
			*NewColumnInfo("price", uint32(INT_32_TYPE), 0, 0, 0, 0),
		}
		metaFilePath := filepath.Join("tables", tableName+".meta")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(metaFilePath)
			os.Remove("tables")
		}()

		// Act
		metaFile, err := CreateMetaFile(tableName, columns)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, metaFile)
		require.NotNil(t, metaFile.Header)
		require.Len(t, metaFile.Columns, 3)

		// Check file exists
		_, err = os.Stat(metaFilePath)
		require.NoError(t, err, "meta file should exist")

		// Check header
		assert.Equal(t, uint32(META_FILE_MAGIC_NUMBER), metaFile.Header.MagicNumber)
		assert.Equal(t, uint32(len(tableName)), metaFile.Header.TableNameLen)
		assert.Equal(t, uint32(3), metaFile.Header.ColumnCount)

		// Check columns
		assert.Equal(t, "id", string(metaFile.Columns[0].ColumnName[:2]))
		assert.Equal(t, INT_32_TYPE, metaFile.Columns[0].DataType)
		assert.Equal(t, uint32(1), metaFile.Columns[0].IsPrimaryKey)

		assert.Equal(t, "name", string(metaFile.Columns[1].ColumnName[:4]))
		assert.Equal(t, TEXT_TYPE, metaFile.Columns[1].DataType)
		assert.Equal(t, uint32(1), metaFile.Columns[1].IsNullable)

		assert.Equal(t, "price", string(metaFile.Columns[2].ColumnName[:5]))
		assert.Equal(t, INT_32_TYPE, metaFile.Columns[2].DataType)
		assert.Equal(t, uint32(0), metaFile.Columns[2].IsNullable)
	})

	t.Run("should create meta file with long table name", func(t *testing.T) {
		// Arrange
		tableName := "very_long_table_name_exceeding_32_chars"
		columns := []ColumnInfo{
			*NewColumnInfo("id", uint32(INT_32_TYPE), 0, 1, 1, 0),
		}
		metaFilePath := filepath.Join("tables", tableName+".meta")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(metaFilePath)
			os.Remove("tables")
		}()

		// Act
		metaFile, err := CreateMetaFile(tableName, columns)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, metaFile)

		// Check file exists
		_, err = os.Stat(metaFilePath)
		require.NoError(t, err, "meta file should exist")

		// Check header - should store original length
		assert.Equal(t, uint32(len(tableName)), metaFile.Header.TableNameLen)
		assert.Equal(t, uint32(1), metaFile.Header.ColumnCount)
	})

	t.Run("should create meta file with empty columns", func(t *testing.T) {
		// Arrange
		tableName := "empty_table"
		columns := []ColumnInfo{}
		metaFilePath := filepath.Join("tables", tableName+".meta")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(metaFilePath)
			os.Remove("tables")
		}()

		// Act
		metaFile, err := CreateMetaFile(tableName, columns)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, metaFile)
		require.NotNil(t, metaFile.Header)
		require.Len(t, metaFile.Columns, 0)

		// Check file exists
		_, err = os.Stat(metaFilePath)
		require.NoError(t, err, "meta file should exist")

		// Check header
		assert.Equal(t, uint32(META_FILE_MAGIC_NUMBER), metaFile.Header.MagicNumber)
		assert.Equal(t, uint32(len(tableName)), metaFile.Header.TableNameLen)
		assert.Equal(t, uint32(0), metaFile.Header.ColumnCount)
	})

	t.Run("should verify file content structure", func(t *testing.T) {
		// Arrange
		tableName := "verify_structure"
		columns := []ColumnInfo{
			*NewColumnInfo("id", uint32(INT_32_TYPE), 0, 1, 1, 0),
			*NewColumnInfo("name", uint32(TEXT_TYPE), 1, 0, 0, 0),
		}
		metaFilePath := filepath.Join("tables", tableName+".meta")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(metaFilePath)
			os.Remove("tables")
		}()

		// Act
		_, err = CreateMetaFile(tableName, columns)

		// Assert
		require.NoError(t, err)

		// Read file content
		fileContent, err := os.ReadFile(metaFilePath)
		require.NoError(t, err)

		// Check file size
		expectedSize := 56 + (52 * 2) // Header + 2 columns
		assert.Equal(t, expectedSize, len(fileContent), "file size should match expected")

		// Verify header content
		assert.Equal(t, uint32(META_FILE_MAGIC_NUMBER), binary.BigEndian.Uint32(fileContent[0:4]))
		assert.Equal(t, uint32(len(tableName)), binary.BigEndian.Uint32(fileContent[4:8]))
		assert.Equal(t, uint32(2), binary.BigEndian.Uint32(fileContent[8:12]))

		// Verify first column content
		firstColumnStart := 56
		assert.Equal(t, "id", string(fileContent[firstColumnStart:firstColumnStart+2]))
		assert.Equal(t, uint32(INT_32_TYPE), binary.BigEndian.Uint32(fileContent[firstColumnStart+32:firstColumnStart+36]))

		// Verify second column content
		secondColumnStart := 56 + 52
		assert.Equal(t, "name", string(fileContent[secondColumnStart:secondColumnStart+4]))
		assert.Equal(t, uint32(TEXT_TYPE), binary.BigEndian.Uint32(fileContent[secondColumnStart+32:secondColumnStart+36]))
	})

	t.Run("should handle file creation error", func(t *testing.T) {
		// Arrange
		tableName := "invalid/path/table"
		columns := []ColumnInfo{
			*NewColumnInfo("id", uint32(INT_32_TYPE), 0, 1, 1, 0),
		}

		// Act
		metaFile, err := CreateMetaFile(tableName, columns)

		// Assert
		require.Error(t, err)
		assert.Nil(t, metaFile)
		assert.Contains(t, err.Error(), "failed to create meta file")
	})
}

func TestReadMetaFile(t *testing.T) {
	t.Run("should read meta file with single column", func(t *testing.T) {
		// Arrange
		tableName := "test_read_users"
		columns := []ColumnInfo{
			*NewColumnInfo("id", uint32(INT_32_TYPE), 0, 1, 1, 0),
		}
		metaFilePath := filepath.Join("tables", tableName+".meta")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(metaFilePath)
			os.Remove("tables")
		}()

		// Create meta file first
		createdMetaFile, err := CreateMetaFile(tableName, columns)
		require.NoError(t, err)
		require.NotNil(t, createdMetaFile)

		// Act
		readMetaFile, err := ReadMetaFile(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, readMetaFile)
		require.NotNil(t, readMetaFile.Header)
		require.Len(t, readMetaFile.Columns, 1)

		// Check header
		assert.Equal(t, uint32(META_FILE_MAGIC_NUMBER), readMetaFile.Header.MagicNumber)
		assert.Equal(t, uint32(len(tableName)), readMetaFile.Header.TableNameLen)
		assert.Equal(t, uint32(1), readMetaFile.Header.ColumnCount)
		assert.Equal(t, uint64(0), readMetaFile.Header.NextRowID)

		// Check column
		assert.Equal(t, "id", string(readMetaFile.Columns[0].ColumnName[:2]))
		assert.Equal(t, INT_32_TYPE, readMetaFile.Columns[0].DataType)
		assert.Equal(t, uint32(0), readMetaFile.Columns[0].IsNullable)
		assert.Equal(t, uint32(1), readMetaFile.Columns[0].IsPrimaryKey)
		assert.Equal(t, uint32(1), readMetaFile.Columns[0].IsAutoIncrement)
		assert.Equal(t, uint32(0), readMetaFile.Columns[0].DefaultValue)
	})

	t.Run("should read meta file with multiple columns", func(t *testing.T) {
		// Arrange
		tableName := "test_read_products"
		columns := []ColumnInfo{
			*NewColumnInfo("id", uint32(INT_32_TYPE), 0, 1, 1, 0),
			*NewColumnInfo("name", uint32(TEXT_TYPE), 1, 0, 0, 0),
			*NewColumnInfo("price", uint32(INT_32_TYPE), 0, 0, 0, 0),
		}
		metaFilePath := filepath.Join("tables", tableName+".meta")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(metaFilePath)
			os.Remove("tables")
		}()

		// Create meta file first
		createdMetaFile, err := CreateMetaFile(tableName, columns)
		require.NoError(t, err)
		require.NotNil(t, createdMetaFile)

		// Act
		readMetaFile, err := ReadMetaFile(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, readMetaFile)
		require.NotNil(t, readMetaFile.Header)
		require.Len(t, readMetaFile.Columns, 3)

		// Check header
		assert.Equal(t, uint32(META_FILE_MAGIC_NUMBER), readMetaFile.Header.MagicNumber)
		assert.Equal(t, uint32(len(tableName)), readMetaFile.Header.TableNameLen)
		assert.Equal(t, uint32(3), readMetaFile.Header.ColumnCount)

		// Check columns
		assert.Equal(t, "id", string(readMetaFile.Columns[0].ColumnName[:2]))
		assert.Equal(t, INT_32_TYPE, readMetaFile.Columns[0].DataType)
		assert.Equal(t, uint32(1), readMetaFile.Columns[0].IsPrimaryKey)

		assert.Equal(t, "name", string(readMetaFile.Columns[1].ColumnName[:4]))
		assert.Equal(t, TEXT_TYPE, readMetaFile.Columns[1].DataType)
		assert.Equal(t, uint32(1), readMetaFile.Columns[1].IsNullable)

		assert.Equal(t, "price", string(readMetaFile.Columns[2].ColumnName[:5]))
		assert.Equal(t, INT_32_TYPE, readMetaFile.Columns[2].DataType)
		assert.Equal(t, uint32(0), readMetaFile.Columns[2].IsNullable)
	})

	t.Run("should read meta file with empty columns", func(t *testing.T) {
		// Arrange
		tableName := "test_read_empty"
		columns := []ColumnInfo{}
		metaFilePath := filepath.Join("tables", tableName+".meta")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(metaFilePath)
			os.Remove("tables")
		}()

		// Create meta file first
		createdMetaFile, err := CreateMetaFile(tableName, columns)
		require.NoError(t, err)
		require.NotNil(t, createdMetaFile)

		// Act
		readMetaFile, err := ReadMetaFile(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, readMetaFile)
		require.NotNil(t, readMetaFile.Header)
		require.Len(t, readMetaFile.Columns, 0)

		// Check header
		assert.Equal(t, uint32(META_FILE_MAGIC_NUMBER), readMetaFile.Header.MagicNumber)
		assert.Equal(t, uint32(len(tableName)), readMetaFile.Header.TableNameLen)
		assert.Equal(t, uint32(0), readMetaFile.Header.ColumnCount)
	})

	t.Run("should read meta file with long table name", func(t *testing.T) {
		// Arrange
		tableName := "very_long_table_name_exceeding_32_chars"
		columns := []ColumnInfo{
			*NewColumnInfo("id", uint32(INT_32_TYPE), 0, 1, 1, 0),
		}
		metaFilePath := filepath.Join("tables", tableName+".meta")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(metaFilePath)
			os.Remove("tables")
		}()

		// Create meta file first
		createdMetaFile, err := CreateMetaFile(tableName, columns)
		require.NoError(t, err)
		require.NotNil(t, createdMetaFile)

		// Act
		readMetaFile, err := ReadMetaFile(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, readMetaFile)
		require.NotNil(t, readMetaFile.Header)

		// Check header - should store original length
		assert.Equal(t, uint32(len(tableName)), readMetaFile.Header.TableNameLen)
		assert.Equal(t, uint32(1), readMetaFile.Header.ColumnCount)
	})

	t.Run("should return error for non-existent table", func(t *testing.T) {
		// Arrange
		tableName := "non_existent_table"

		// Act
		readMetaFile, err := ReadMetaFile(tableName)

		// Assert
		require.Error(t, err)
		assert.Nil(t, readMetaFile)
		assert.Contains(t, err.Error(), "table non_existent_table not found")
	})

	t.Run("should return error for corrupted header", func(t *testing.T) {
		// Arrange
		tableName := "test_corrupted_header"
		metaFilePath := filepath.Join("tables", tableName+".meta")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(metaFilePath)
			os.Remove("tables")
		}()

		// Create a corrupted file with insufficient header data
		file, err := os.Create(metaFilePath)
		require.NoError(t, err)

		// Write only 10 bytes instead of 56
		corruptedData := make([]byte, 10)
		_, err = file.Write(corruptedData)
		require.NoError(t, err)
		file.Close()

		// Act
		readMetaFile, err := ReadMetaFile(tableName)

		// Assert
		require.Error(t, err)
		assert.Nil(t, readMetaFile)
		assert.Contains(t, err.Error(), "incomplete header read")
	})

	t.Run("should return error for corrupted column data", func(t *testing.T) {
		// Arrange
		tableName := "test_corrupted_column"
		metaFilePath := filepath.Join("tables", tableName+".meta")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(metaFilePath)
			os.Remove("tables")
		}()

		// Create a file with valid header but corrupted column data
		file, err := os.Create(metaFilePath)
		require.NoError(t, err)

		// Write valid header (56 bytes)
		header := NewMetaFileHeader(tableName, 1)
		_, err = file.Write(header.Serialize())
		require.NoError(t, err)

		// Write only 10 bytes instead of 52 for column
		corruptedColumnData := make([]byte, 10)
		_, err = file.Write(corruptedColumnData)
		require.NoError(t, err)
		file.Close()

		// Act
		readMetaFile, err := ReadMetaFile(tableName)

		// Assert
		require.Error(t, err)
		assert.Nil(t, readMetaFile)
		assert.Contains(t, err.Error(), "incomplete column 0 read")
	})

	t.Run("should handle deserialization errors", func(t *testing.T) {
		// Arrange
		tableName := "test_deserialize_error"
		metaFilePath := filepath.Join("tables", tableName+".meta")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(metaFilePath)
			os.Remove("tables")
		}()

		// Create a file with invalid header data
		file, err := os.Create(metaFilePath)
		require.NoError(t, err)

		// Write invalid header data (all zeros, which should cause deserialization issues)
		invalidHeaderData := make([]byte, 56)
		_, err = file.Write(invalidHeaderData)
		require.NoError(t, err)
		file.Close()

		// Act
		readMetaFile, err := ReadMetaFile(tableName)

		// Assert
		require.NoError(t, err) // Deserialization should still work with all zeros
		require.NotNil(t, readMetaFile)
		assert.Equal(t, uint32(0), readMetaFile.Header.MagicNumber)
		assert.Equal(t, uint32(0), readMetaFile.Header.ColumnCount)
	})

	t.Run("should verify data integrity through create-read cycle", func(t *testing.T) {
		// Arrange
		tableName := "test_integrity"
		originalColumns := []ColumnInfo{
			*NewColumnInfo("id", uint32(INT_32_TYPE), 0, 1, 1, 0),
			*NewColumnInfo("name", uint32(TEXT_TYPE), 1, 0, 0, 42),
			*NewColumnInfo("active", uint32(INT_32_TYPE), 0, 0, 0, 1),
		}
		metaFilePath := filepath.Join("tables", tableName+".meta")

		// Ensure tables directory exists
		err := os.MkdirAll("tables", 0755)
		require.NoError(t, err)

		// Clean up after test
		defer func() {
			os.Remove(metaFilePath)
			os.Remove("tables")
		}()

		// Create meta file
		createdMetaFile, err := CreateMetaFile(tableName, originalColumns)
		require.NoError(t, err)
		require.NotNil(t, createdMetaFile)

		// Act - Read meta file
		readMetaFile, err := ReadMetaFile(tableName)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, readMetaFile)

		// Verify header integrity
		assert.Equal(t, createdMetaFile.Header.MagicNumber, readMetaFile.Header.MagicNumber)
		assert.Equal(t, createdMetaFile.Header.TableNameLen, readMetaFile.Header.TableNameLen)
		assert.Equal(t, createdMetaFile.Header.ColumnCount, readMetaFile.Header.ColumnCount)
		assert.Equal(t, createdMetaFile.Header.NextRowID, readMetaFile.Header.NextRowID)

		// Verify columns integrity
		require.Len(t, readMetaFile.Columns, len(originalColumns))
		for i, originalColumn := range originalColumns {
			readColumn := readMetaFile.Columns[i]
			assert.Equal(t, originalColumn.ColumnName, readColumn.ColumnName)
			assert.Equal(t, originalColumn.DataType, readColumn.DataType)
			assert.Equal(t, originalColumn.IsNullable, readColumn.IsNullable)
			assert.Equal(t, originalColumn.IsPrimaryKey, readColumn.IsPrimaryKey)
			assert.Equal(t, originalColumn.IsAutoIncrement, readColumn.IsAutoIncrement)
			assert.Equal(t, originalColumn.DefaultValue, readColumn.DefaultValue)
		}
	})
}
