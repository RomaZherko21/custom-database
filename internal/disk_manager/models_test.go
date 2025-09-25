package disk_manager

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRowGetSize(t *testing.T) {
	t.Run("1. Row size calculation with INT_32_TYPE cells", func(t *testing.T) {
		// Arrange
		row := Row{
			{DataType: INT_32_TYPE, Data: int32(100), IsNull: false},
			{DataType: INT_32_TYPE, Data: int32(200), IsNull: false},
		}

		// Act
		size := row.GetSize()

		// Assert
		expectedSize := uint32(2*4) + TUPLE_LENGTH_FIELD_SIZE + TUPLE_NULL_BITMAP_SIZE + 1 // 2 cells * 4 bytes + header + 1 byte null bitmap
		require.Equal(t, expectedSize, size)
	})

	t.Run("2. Row size calculation with TEXT_TYPE cells", func(t *testing.T) {
		// Arrange
		row := Row{
			{DataType: TEXT_TYPE, Data: "hello", IsNull: false},
			{DataType: TEXT_TYPE, Data: "world", IsNull: false},
		}

		// Act
		size := row.GetSize()

		// Assert
		expectedSize := uint32(2*(4+5)) + TUPLE_LENGTH_FIELD_SIZE + TUPLE_NULL_BITMAP_SIZE + 1 // 2 cells * (4 bytes length + 5 bytes data) + header + 1 byte null bitmap
		require.Equal(t, expectedSize, size)
	})

	t.Run("3. Row size calculation with NULL cells", func(t *testing.T) {
		// Arrange
		row := Row{
			{DataType: INT_32_TYPE, Data: int32(100), IsNull: false},
			{DataType: TEXT_TYPE, Data: "test", IsNull: true},
		}

		// Act
		size := row.GetSize()

		// Assert
		expectedSize := uint32(4) + TUPLE_LENGTH_FIELD_SIZE + TUPLE_NULL_BITMAP_SIZE + 1 // Only first cell contributes data size + header + 1 byte null bitmap
		require.Equal(t, expectedSize, size)
	})

	t.Run("4. Row size calculation with empty row", func(t *testing.T) {
		// Arrange
		row := Row{}

		// Act
		size := row.GetSize()

		// Assert
		expectedSize := uint32(TUPLE_LENGTH_FIELD_SIZE + TUPLE_NULL_BITMAP_SIZE + 1) // header + 1 byte null bitmap
		require.Equal(t, expectedSize, size)
	})
}

func TestDataCellSerializeData(t *testing.T) {
	t.Run("1. Serialize INT_32_TYPE data", func(t *testing.T) {
		// Arrange
		cell := &DataCell{
			DataType: INT_32_TYPE,
			Data:     int32(12345),
			IsNull:   false,
		}

		// Act
		data := cell.SerializeData()

		// Assert
		require.Len(t, data, 4)
		require.Equal(t, int32(12345), int32(binary.BigEndian.Uint32(data)))
	})

	t.Run("2. Serialize TEXT_TYPE data", func(t *testing.T) {
		// Arrange
		cell := &DataCell{
			DataType: TEXT_TYPE,
			Data:     "hello",
			IsNull:   false,
		}

		// Act
		data := cell.SerializeData()

		// Assert
		require.Len(t, data, 9) // 4 bytes length + 5 bytes data
		require.Equal(t, uint32(5), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, "hello", string(data[4:]))
	})

	t.Run("3. Serialize NULL data", func(t *testing.T) {
		// Arrange
		cell := &DataCell{
			DataType: INT_32_TYPE,
			Data:     int32(100),
			IsNull:   true,
		}

		// Act
		data := cell.SerializeData()

		// Assert
		require.Empty(t, data)
	})

	t.Run("4. Serialize empty TEXT_TYPE data", func(t *testing.T) {
		// Arrange
		cell := &DataCell{
			DataType: TEXT_TYPE,
			Data:     "",
			IsNull:   false,
		}

		// Act
		data := cell.SerializeData()

		// Assert
		require.Len(t, data, 4) // Only length field
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[0:4]))
	})
}

func TestDataCellSerializeInt32(t *testing.T) {
	t.Run("1. Serialize positive int32", func(t *testing.T) {
		// Arrange
		cell := &DataCell{
			DataType: INT_32_TYPE,
			Data:     int32(12345),
			IsNull:   false,
		}

		// Act
		data := cell.serializeInt32()

		// Assert
		require.Len(t, data, 4)
		require.Equal(t, int32(12345), int32(binary.BigEndian.Uint32(data)))
	})

	t.Run("2. Serialize negative int32", func(t *testing.T) {
		// Arrange
		cell := &DataCell{
			DataType: INT_32_TYPE,
			Data:     int32(-12345),
			IsNull:   false,
		}

		// Act
		data := cell.serializeInt32()

		// Assert
		require.Len(t, data, 4)
		require.Equal(t, int32(-12345), int32(binary.BigEndian.Uint32(data)))
	})

	t.Run("3. Serialize zero int32", func(t *testing.T) {
		// Arrange
		cell := &DataCell{
			DataType: INT_32_TYPE,
			Data:     int32(0),
			IsNull:   false,
		}

		// Act
		data := cell.serializeInt32()

		// Assert
		require.Len(t, data, 4)
		require.Equal(t, int32(0), int32(binary.BigEndian.Uint32(data)))
	})
}

func TestDataCellSerializeText(t *testing.T) {
	t.Run("1. Serialize normal text", func(t *testing.T) {
		// Arrange
		cell := &DataCell{
			DataType: TEXT_TYPE,
			Data:     "hello world",
			IsNull:   false,
		}

		// Act
		data := cell.serializeText()

		// Assert
		require.Len(t, data, 15) // 4 bytes length + 11 bytes data
		require.Equal(t, uint32(11), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, "hello world", string(data[4:]))
	})

	t.Run("2. Serialize empty text", func(t *testing.T) {
		// Arrange
		cell := &DataCell{
			DataType: TEXT_TYPE,
			Data:     "",
			IsNull:   false,
		}

		// Act
		data := cell.serializeText()

		// Assert
		require.Len(t, data, 4) // Only length field
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[0:4]))
	})

	t.Run("3. Serialize text with special characters", func(t *testing.T) {
		// Arrange
		cell := &DataCell{
			DataType: TEXT_TYPE,
			Data:     "тест 123 !@#$%",
			IsNull:   false,
		}

		// Act
		data := cell.serializeText()

		// Assert
		expectedLength := uint32(len("тест 123 !@#$%"))
		require.Len(t, data, int(4+expectedLength))
		require.Equal(t, expectedLength, binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, "тест 123 !@#$%", string(data[4:]))
	})
}

func TestDataCellGetSize(t *testing.T) {
	t.Run("1. Get size for INT_32_TYPE", func(t *testing.T) {
		// Arrange
		cell := &DataCell{
			DataType: INT_32_TYPE,
			Data:     int32(100),
			IsNull:   false,
		}

		// Act
		size := cell.GetSize()

		// Assert
		require.Equal(t, uint32(4), size)
	})

	t.Run("2. Get size for TEXT_TYPE", func(t *testing.T) {
		// Arrange
		cell := &DataCell{
			DataType: TEXT_TYPE,
			Data:     "hello",
			IsNull:   false,
		}

		// Act
		size := cell.GetSize()

		// Assert
		require.Equal(t, uint32(9), size) // 4 bytes length + 5 bytes data
	})

	t.Run("3. Get size for NULL cell", func(t *testing.T) {
		// Arrange
		cell := &DataCell{
			DataType: INT_32_TYPE,
			Data:     int32(100),
			IsNull:   true,
		}

		// Act
		size := cell.GetSize()

		// Assert
		require.Equal(t, uint32(0), size)
	})

	t.Run("4. Get size for empty TEXT_TYPE", func(t *testing.T) {
		// Arrange
		cell := &DataCell{
			DataType: TEXT_TYPE,
			Data:     "",
			IsNull:   false,
		}

		// Act
		size := cell.GetSize()

		// Assert
		require.Equal(t, uint32(4), size) // Only length field
	})
}

func TestDeserializeDataCell(t *testing.T) {
	t.Run("1. Deserialize INT_32_TYPE", func(t *testing.T) {
		// Arrange
		data := make([]byte, 4)
		binary.BigEndian.PutUint32(data, 12345)

		// Act
		cell, err := DeserializeDataCell(data, INT_32_TYPE, false)

		// Assert
		require.NoError(t, err)
		require.Equal(t, INT_32_TYPE, cell.DataType)
		require.Equal(t, int32(12345), cell.Data)
		require.False(t, cell.IsNull)
	})

	t.Run("2. Deserialize TEXT_TYPE", func(t *testing.T) {
		// Arrange
		text := "hello world"
		data := make([]byte, 4+len(text))
		binary.BigEndian.PutUint32(data[0:4], uint32(len(text)))
		copy(data[4:], text)

		// Act
		cell, err := DeserializeDataCell(data, TEXT_TYPE, false)

		// Assert
		require.NoError(t, err)
		require.Equal(t, TEXT_TYPE, cell.DataType)
		require.Equal(t, "hello world", cell.Data)
		require.False(t, cell.IsNull)
	})

	t.Run("3. Deserialize NULL cell", func(t *testing.T) {
		// Arrange
		data := []byte{}

		// Act
		cell, err := DeserializeDataCell(data, INT_32_TYPE, true)

		// Assert
		require.NoError(t, err)
		require.Equal(t, INT_32_TYPE, cell.DataType)
		require.Nil(t, cell.Data)
		require.True(t, cell.IsNull)
	})

	t.Run("4. Deserialize INT_32_TYPE with insufficient data", func(t *testing.T) {
		// Arrange
		data := []byte{1, 2} // Only 2 bytes instead of 4

		// Act
		cell, err := DeserializeDataCell(data, INT_32_TYPE, false)

		// Assert
		require.Error(t, err)
		require.Nil(t, cell)
		require.Contains(t, err.Error(), "insufficient data")
	})

	t.Run("5. Deserialize TEXT_TYPE with insufficient length data", func(t *testing.T) {
		// Arrange
		data := []byte{1, 2} // Only 2 bytes instead of 4 for length

		// Act
		cell, err := DeserializeDataCell(data, TEXT_TYPE, false)

		// Assert
		require.Error(t, err)
		require.Nil(t, cell)
		require.Contains(t, err.Error(), "insufficient data")
	})

	t.Run("6. Deserialize TEXT_TYPE with insufficient content data", func(t *testing.T) {
		// Arrange
		data := make([]byte, 6) // Length says 10 bytes, but only 6 total
		binary.BigEndian.PutUint32(data[0:4], 10)

		// Act
		cell, err := DeserializeDataCell(data, TEXT_TYPE, false)

		// Assert
		require.Error(t, err)
		require.Nil(t, cell)
		require.Contains(t, err.Error(), "insufficient data")
	})

	t.Run("7. Deserialize unsupported data type", func(t *testing.T) {
		// Arrange
		data := []byte{1, 2, 3, 4}

		// Act
		cell, err := DeserializeDataCell(data, DataType(999), false)

		// Assert
		require.Error(t, err)
		require.Nil(t, cell)
		require.Contains(t, err.Error(), "unsupported data type")
	})
}

func TestDataCellDeserializeInt32(t *testing.T) {
	t.Run("1. Deserialize positive int32", func(t *testing.T) {
		// Arrange
		cell := &DataCell{DataType: INT_32_TYPE}
		data := make([]byte, 4)
		binary.BigEndian.PutUint32(data, 12345)

		// Act
		result, err := cell.deserializeInt32(data)

		// Assert
		require.NoError(t, err)
		require.Equal(t, int32(12345), result.Data)
	})

	t.Run("2. Deserialize negative int32", func(t *testing.T) {
		// Arrange
		cell := &DataCell{DataType: INT_32_TYPE}
		data := make([]byte, 4)
		negativeValue := int32(-12345)
		binary.BigEndian.PutUint32(data, uint32(negativeValue))

		// Act
		result, err := cell.deserializeInt32(data)

		// Assert
		require.NoError(t, err)
		require.Equal(t, int32(-12345), result.Data)
	})

	t.Run("3. Deserialize int32 with insufficient data", func(t *testing.T) {
		// Arrange
		cell := &DataCell{DataType: INT_32_TYPE}
		data := []byte{1, 2} // Only 2 bytes

		// Act
		result, err := cell.deserializeInt32(data)

		// Assert
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "insufficient data")
	})
}

func TestDataCellDeserializeText(t *testing.T) {
	t.Run("1. Deserialize normal text", func(t *testing.T) {
		// Arrange
		cell := &DataCell{DataType: TEXT_TYPE}
		text := "hello world"
		data := make([]byte, 4+len(text))
		binary.BigEndian.PutUint32(data[0:4], uint32(len(text)))
		copy(data[4:], text)

		// Act
		result, err := cell.deserializeText(data)

		// Assert
		require.NoError(t, err)
		require.Equal(t, "hello world", result.Data)
	})

	t.Run("2. Deserialize empty text", func(t *testing.T) {
		// Arrange
		cell := &DataCell{DataType: TEXT_TYPE}
		data := make([]byte, 4)
		binary.BigEndian.PutUint32(data, 0)

		// Act
		result, err := cell.deserializeText(data)

		// Assert
		require.NoError(t, err)
		require.Equal(t, "", result.Data)
	})

	t.Run("3. Deserialize text with insufficient length data", func(t *testing.T) {
		// Arrange
		cell := &DataCell{DataType: TEXT_TYPE}
		data := []byte{1, 2} // Only 2 bytes for length

		// Act
		result, err := cell.deserializeText(data)

		// Assert
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "insufficient data")
	})

	t.Run("4. Deserialize text with insufficient content data", func(t *testing.T) {
		// Arrange
		cell := &DataCell{DataType: TEXT_TYPE}
		data := make([]byte, 6) // Length says 10 bytes, but only 6 total
		binary.BigEndian.PutUint32(data[0:4], 10)

		// Act
		result, err := cell.deserializeText(data)

		// Assert
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "insufficient data")
	})
}

func TestConvertRawTupleToRow(t *testing.T) {
	t.Run("1. Convert RawTuple to Row with INT_32_TYPE columns", func(t *testing.T) {
		// Arrange
		rawTuple := RawTuple{
			Length:         12, // 4 bytes length + 4 bytes null bitmap + 4 bytes data
			NullBitmapSize: 1,
			NullBitmap:     []byte{0},
			Data:           make([]byte, 4),
		}
		binary.BigEndian.PutUint32(rawTuple.Data, 12345)

		columns := []ColumnInfo{
			{DataType: INT_32_TYPE},
		}

		// Act
		row, err := ConvertRawTupleToRow(rawTuple, columns)

		// Assert
		require.NoError(t, err)
		require.Len(t, row, 1)
		require.Equal(t, INT_32_TYPE, row[0].DataType)
		require.Equal(t, int32(12345), row[0].Data)
		require.False(t, row[0].IsNull)
	})

	t.Run("2. Convert RawTuple to Row with TEXT_TYPE columns", func(t *testing.T) {
		// Arrange
		text := "hello"
		rawTuple := RawTuple{
			Length:         uint32(4 + 4 + 4 + len(text)), // length + null bitmap + data length + text
			NullBitmapSize: 1,
			NullBitmap:     []byte{0},
			Data:           make([]byte, 4+len(text)),
		}
		binary.BigEndian.PutUint32(rawTuple.Data[0:4], uint32(len(text)))
		copy(rawTuple.Data[4:], text)

		columns := []ColumnInfo{
			{DataType: TEXT_TYPE},
		}

		// Act
		row, err := ConvertRawTupleToRow(rawTuple, columns)

		// Assert
		require.NoError(t, err)
		require.Len(t, row, 1)
		require.Equal(t, TEXT_TYPE, row[0].DataType)
		require.Equal(t, "hello", row[0].Data)
		require.False(t, row[0].IsNull)
	})

	t.Run("3. Convert RawTuple to Row with NULL values", func(t *testing.T) {
		// Arrange
		rawTuple := RawTuple{
			Length:         8, // 4 bytes length + 4 bytes null bitmap
			NullBitmapSize: 1,
			NullBitmap:     []byte{1},
			Data:           []byte{},
		}

		columns := []ColumnInfo{
			{DataType: INT_32_TYPE},
		}

		// Act
		row, err := ConvertRawTupleToRow(rawTuple, columns)

		// Assert
		require.NoError(t, err)
		require.Len(t, row, 1)
		require.Equal(t, INT_32_TYPE, row[0].DataType)
		require.Nil(t, row[0].Data)
		require.True(t, row[0].IsNull)
	})

	t.Run("4. Convert RawTuple to Row with multiple columns", func(t *testing.T) {
		// Arrange
		rawTuple := RawTuple{
			Length:         13,        // 4 bytes length + 4 bytes null bitmap size + 1 byte null bitmap + 4 bytes data
			NullBitmapSize: 1,         // 1 байт для 2 колонок
			NullBitmap:     []byte{2}, // бит 1 установлен (колонка 1 = NULL)
			Data:           make([]byte, 4),
		}
		binary.BigEndian.PutUint32(rawTuple.Data[0:4], 12345)

		columns := []ColumnInfo{
			{DataType: INT_32_TYPE},
			{DataType: INT_32_TYPE},
		}

		// Act
		row, err := ConvertRawTupleToRow(rawTuple, columns)

		// Assert
		require.NoError(t, err)
		require.Len(t, row, 2)
		require.Equal(t, int32(12345), row[0].Data)
		require.False(t, row[0].IsNull)
		require.Nil(t, row[1].Data)
		require.True(t, row[1].IsNull)
	})

	t.Run("5. Convert RawTuple to Row with deserialization error", func(t *testing.T) {
		// Arrange
		rawTuple := RawTuple{
			Length:         8, // Insufficient data
			NullBitmapSize: 1,
			NullBitmap:     []byte{0},
			Data:           []byte{1, 2}, // Only 2 bytes instead of 4
		}

		columns := []ColumnInfo{
			{DataType: INT_32_TYPE},
		}

		// Act
		row, err := ConvertRawTupleToRow(rawTuple, columns)

		// Assert
		require.Error(t, err)
		require.Nil(t, row)
		require.Contains(t, err.Error(), "insufficient data for column")
	})
}

func TestConvertRowToRawTuple(t *testing.T) {
	t.Run("1. Convert Row to RawTuple with INT_32_TYPE cells", func(t *testing.T) {
		// Arrange
		row := Row{
			{DataType: INT_32_TYPE, Data: int32(12345), IsNull: false},
		}

		// Act
		rawTuple := ConvertRowToRawTuple(row)

		// Assert
		require.Equal(t, row.GetSize(), rawTuple.Length)
		require.Equal(t, uint32(1), rawTuple.NullBitmapSize)
		require.Equal(t, []byte{0}, rawTuple.NullBitmap)
		require.Len(t, rawTuple.Data, 4)
		require.Equal(t, int32(12345), int32(binary.BigEndian.Uint32(rawTuple.Data)))
	})

	t.Run("2. Convert Row to RawTuple with TEXT_TYPE cells", func(t *testing.T) {
		// Arrange
		row := Row{
			{DataType: TEXT_TYPE, Data: "hello", IsNull: false},
		}

		// Act
		rawTuple := ConvertRowToRawTuple(row)

		// Assert
		require.Equal(t, row.GetSize(), rawTuple.Length)
		require.Equal(t, uint32(1), rawTuple.NullBitmapSize)
		require.Equal(t, []byte{0}, rawTuple.NullBitmap)
		require.Len(t, rawTuple.Data, 9) // 4 bytes length + 5 bytes data
		require.Equal(t, uint32(5), binary.BigEndian.Uint32(rawTuple.Data[0:4]))
		require.Equal(t, "hello", string(rawTuple.Data[4:]))
	})

	t.Run("3. Convert Row to RawTuple with NULL cells", func(t *testing.T) {
		// Arrange
		row := Row{
			{DataType: INT_32_TYPE, Data: int32(12345), IsNull: true},
		}

		// Act
		rawTuple := ConvertRowToRawTuple(row)

		// Assert
		require.Equal(t, row.GetSize(), rawTuple.Length)
		require.Equal(t, uint32(1), rawTuple.NullBitmapSize)
		require.Equal(t, []byte{1}, rawTuple.NullBitmap)
		require.Empty(t, rawTuple.Data)
	})

	t.Run("4. Convert Row to RawTuple with mixed cells", func(t *testing.T) {
		// Arrange
		row := Row{
			{DataType: INT_32_TYPE, Data: int32(12345), IsNull: false},
			{DataType: TEXT_TYPE, Data: "test", IsNull: true},
		}

		// Act
		rawTuple := ConvertRowToRawTuple(row)

		// Assert
		require.Equal(t, row.GetSize(), rawTuple.Length)
		require.Equal(t, uint32(1), rawTuple.NullBitmapSize) // 1 байт для 2 колонок
		require.Equal(t, []byte{2}, rawTuple.NullBitmap)     // бит 1 установлен (колонка 1 = NULL)
		require.Len(t, rawTuple.Data, 4)                     // Only first cell contributes data
		require.Equal(t, int32(12345), int32(binary.BigEndian.Uint32(rawTuple.Data)))
	})

	t.Run("5. Convert empty Row to RawTuple", func(t *testing.T) {
		// Arrange
		row := Row{}

		// Act
		rawTuple := ConvertRowToRawTuple(row)

		// Assert
		require.Equal(t, row.GetSize(), rawTuple.Length)
		require.Equal(t, uint32(1), rawTuple.NullBitmapSize) // Минимальный размер 1 байт
		require.Equal(t, []byte{0}, rawTuple.NullBitmap)     // Все биты сброшены
		require.Empty(t, rawTuple.Data)
	})
}

func TestConvertPageToRawPage(t *testing.T) {
	t.Run("1. Convert Page to RawPage with single row", func(t *testing.T) {
		// Arrange
		page := &Page{
			Header: PageHeader{
				PageID:      1,
				RecordCount: 1,
				Lower:       PAGE_HEADER_SIZE,
				Upper:       PAGE_SIZE,
			},
			Slots: []PageSlot{
				{Offset: 100, Length: 20, Flags: 0},
			},
			Rows: []Row{
				{
					{DataType: INT_32_TYPE, Data: int32(12345), IsNull: false},
				},
			},
			Columns: []ColumnInfo{
				{DataType: INT_32_TYPE},
			},
		}

		// Act
		rawPage := ConvertPageToRawPage(page)

		// Assert
		require.Equal(t, page.Header, rawPage.Header)
		require.Equal(t, page.Slots, rawPage.Slots)
		require.Len(t, rawPage.RawTuples, 1)
		require.Equal(t, page.Rows[0].GetSize(), rawPage.RawTuples[0].Length)
	})

	t.Run("2. Convert Page to RawPage with multiple rows", func(t *testing.T) {
		// Arrange
		page := &Page{
			Header: PageHeader{
				PageID:      1,
				RecordCount: 2,
				Lower:       PAGE_HEADER_SIZE,
				Upper:       PAGE_SIZE,
			},
			Slots: []PageSlot{
				{Offset: 100, Length: 20, Flags: 0},
				{Offset: 120, Length: 25, Flags: 0},
			},
			Rows: []Row{
				{
					{DataType: INT_32_TYPE, Data: int32(12345), IsNull: false},
				},
				{
					{DataType: TEXT_TYPE, Data: "hello", IsNull: false},
				},
			},
			Columns: []ColumnInfo{
				{DataType: INT_32_TYPE},
			},
		}

		// Act
		rawPage := ConvertPageToRawPage(page)

		// Assert
		require.Equal(t, page.Header, rawPage.Header)
		require.Equal(t, page.Slots, rawPage.Slots)
		require.Len(t, rawPage.RawTuples, 2)
		require.Equal(t, page.Rows[0].GetSize(), rawPage.RawTuples[0].Length)
		require.Equal(t, page.Rows[1].GetSize(), rawPage.RawTuples[1].Length)
	})

	t.Run("3. Convert empty Page to RawPage", func(t *testing.T) {
		// Arrange
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
		rawPage := ConvertPageToRawPage(page)

		// Assert
		require.Equal(t, page.Header, rawPage.Header)
		require.Equal(t, page.Slots, rawPage.Slots)
		require.Empty(t, rawPage.RawTuples)
	})
}

func TestCalculateNullBitmapSize(t *testing.T) {
	tests := []struct {
		name         string
		columnCount  int
		expectedSize uint32
		expectError  bool
	}{
		{"1 column", 1, 1, false},
		{"2 columns", 2, 1, false},
		{"8 columns", 8, 1, false},
		{"9 columns", 9, 2, false},
		{"16 columns", 16, 2, false},
		{"17 columns", 17, 3, false},
		{"24 columns", 24, 3, false},
		{"25 columns", 25, 4, false},
		{"32 columns", 32, 4, false},
		{"0 columns", 0, 0, true},
		{"33 columns", 33, 0, true},
		{"100 columns", 100, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size, err := CalculateNullBitmapSize(tt.columnCount)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedSize, size)
			}
		})
	}
}
