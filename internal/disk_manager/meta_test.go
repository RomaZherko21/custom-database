package disk_manager

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetaFileHeader_Serialize(t *testing.T) {
	t.Run("should serialize meta file header correctly", func(t *testing.T) {
		// Arrange
		meta := &MetaFileHeader{
			MagicNumber:  0x9ABCDEF0,
			TableNameLen: 5,
			ColumnCount:  3,
			TableName:    [32]byte{'u', 's', 'e', 'r', 's', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			NextRowID:    42,
		}

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
		meta := &MetaFileHeader{
			MagicNumber:  0,
			TableNameLen: 0,
			ColumnCount:  0,
			TableName:    [32]byte{},
			NextRowID:    0,
		}

		// Act
		result := meta.Serialize()

		// Assert
		require.NotNil(t, result)
		assert.Equal(t, 56, len(result))

		// Проверяем, что все поля равны нулю
		assert.Equal(t, uint32(0), binary.BigEndian.Uint32(result[0:4]))
		assert.Equal(t, uint32(0), binary.BigEndian.Uint32(result[4:8]))
		assert.Equal(t, uint32(0), binary.BigEndian.Uint32(result[8:12]))
		assert.Equal(t, [32]byte{}, [32]byte(result[12:44]))
		assert.Equal(t, uint64(0), binary.BigEndian.Uint64(result[44:52]))
	})

	t.Run("should serialize with maximum values", func(t *testing.T) {
		// Arrange
		meta := &MetaFileHeader{
			MagicNumber:  0xFFFFFFFF,
			TableNameLen: 0xFFFFFFFF,
			ColumnCount:  0xFFFFFFFF,
			TableName:    [32]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			NextRowID:    0xFFFFFFFFFFFFFFFF,
		}

		// Act
		result := meta.Serialize()

		// Assert
		require.NotNil(t, result)
		assert.Equal(t, 56, len(result))

		// Проверяем максимальные значения
		assert.Equal(t, uint32(0xFFFFFFFF), binary.BigEndian.Uint32(result[0:4]))
		assert.Equal(t, uint32(0xFFFFFFFF), binary.BigEndian.Uint32(result[4:8]))
		assert.Equal(t, uint32(0xFFFFFFFF), binary.BigEndian.Uint32(result[8:12]))
		assert.Equal(t, [32]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, [32]byte(result[12:44]))
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
		original := &MetaFileHeader{
			MagicNumber:  0x9ABCDEF0,
			TableNameLen: 5,
			ColumnCount:  3,
			TableName:    [32]byte{'u', 's', 'e', 'r', 's', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			NextRowID:    42,
		}

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
		original := &MetaFileHeader{
			MagicNumber:  0,
			TableNameLen: 0,
			ColumnCount:  0,
			TableName:    [32]byte{},
			NextRowID:    0,
		}

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
		original := &MetaFileHeader{
			MagicNumber:  0xFFFFFFFF,
			TableNameLen: 0xFFFFFFFF,
			ColumnCount:  0xFFFFFFFF,
			TableName:    [32]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			NextRowID:    0xFFFFFFFFFFFFFFFF,
		}

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
