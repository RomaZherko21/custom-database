package disk_manager

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSerializePageHeader(t *testing.T) {
	t.Run("1. Page header serialization success", func(t *testing.T) {
		// Arrange
		header := &PageHeader{
			PageID:      1,
			RecordCount: 5,
			Lower:       16,
			Upper:       4000,
		}

		// Act
		data := header.Serialize()

		// Assert
		require.Len(t, data, PAGE_HEADER_SIZE)
		require.Equal(t, uint32(1), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(5), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(16), binary.BigEndian.Uint32(data[8:12]))
		require.Equal(t, uint32(4000), binary.BigEndian.Uint32(data[12:16]))
	})

	t.Run("2. Page header serialization with zero records", func(t *testing.T) {
		// Arrange
		header := &PageHeader{
			PageID:      0,
			RecordCount: 0,
			Lower:       PAGE_HEADER_SIZE,
			Upper:       PAGE_SIZE,
		}

		// Act
		data := header.Serialize()

		// Assert
		require.Len(t, data, PAGE_HEADER_SIZE)
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(PAGE_HEADER_SIZE), binary.BigEndian.Uint32(data[8:12]))
		require.Equal(t, uint32(PAGE_SIZE), binary.BigEndian.Uint32(data[12:16]))
	})

	t.Run("3. Page header serialization with large values", func(t *testing.T) {
		// Arrange
		header := &PageHeader{
			PageID:      1000,
			RecordCount: 100,
			Lower:       200,
			Upper:       3000,
		}

		// Act
		data := header.Serialize()

		// Assert
		require.Len(t, data, PAGE_HEADER_SIZE)
		require.Equal(t, uint32(1000), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(100), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(200), binary.BigEndian.Uint32(data[8:12]))
		require.Equal(t, uint32(3000), binary.BigEndian.Uint32(data[12:16]))
	})
}

func TestDeserializePageHeader(t *testing.T) {
	t.Run("1. Page header deserialization success", func(t *testing.T) {
		// Arrange
		originalHeader := &PageHeader{
			PageID:      2,
			RecordCount: 3,
			Lower:       20,
			Upper:       3500,
		}
		data := originalHeader.Serialize()

		// Act
		deserializedHeader, err := (&PageHeader{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.Equal(t, originalHeader.PageID, deserializedHeader.PageID)
		require.Equal(t, originalHeader.RecordCount, deserializedHeader.RecordCount)
		require.Equal(t, originalHeader.Lower, deserializedHeader.Lower)
		require.Equal(t, originalHeader.Upper, deserializedHeader.Upper)
	})

	t.Run("2. Page header deserialization with insufficient data", func(t *testing.T) {
		// Arrange
		insufficientData := make([]byte, PAGE_HEADER_SIZE-1)

		// Act
		header, err := (&PageHeader{}).Deserialize(insufficientData)

		// Assert
		require.Error(t, err)
		require.Nil(t, header)
		require.Contains(t, err.Error(), "insufficient data for page header")
	})

	t.Run("3. Page header deserialization with empty data", func(t *testing.T) {
		// Arrange
		emptyData := make([]byte, 0)

		// Act
		header, err := (&PageHeader{}).Deserialize(emptyData)

		// Assert
		require.Error(t, err)
		require.Nil(t, header)
		require.Contains(t, err.Error(), "insufficient data for page header")
	})
}

func TestNewPageHeader(t *testing.T) {
	t.Run("1. New page header creation success", func(t *testing.T) {
		// Arrange
		pageID := uint32(5)

		// Act
		header := newPageHeader(pageID)

		// Assert
		require.NotNil(t, header)
		require.Equal(t, pageID, header.PageID)
		require.Equal(t, uint32(0), header.RecordCount)
		require.Equal(t, uint32(PAGE_HEADER_SIZE), header.Lower)
		require.Equal(t, uint32(PAGE_SIZE), header.Upper)
	})

	t.Run("2. New page header creation with zero page ID", func(t *testing.T) {
		// Arrange
		pageID := uint32(0)

		// Act
		header := newPageHeader(pageID)

		// Assert
		require.NotNil(t, header)
		require.Equal(t, pageID, header.PageID)
		require.Equal(t, uint32(0), header.RecordCount)
		require.Equal(t, uint32(PAGE_HEADER_SIZE), header.Lower)
		require.Equal(t, uint32(PAGE_SIZE), header.Upper)
	})

	t.Run("3. New page header creation with large page ID", func(t *testing.T) {
		// Arrange
		pageID := uint32(9999)

		// Act
		header := newPageHeader(pageID)

		// Assert
		require.NotNil(t, header)
		require.Equal(t, pageID, header.PageID)
		require.Equal(t, uint32(0), header.RecordCount)
		require.Equal(t, uint32(PAGE_HEADER_SIZE), header.Lower)
		require.Equal(t, uint32(PAGE_SIZE), header.Upper)
	})
}

func TestSerializePageSlot(t *testing.T) {
	t.Run("1. Page slot serialization success", func(t *testing.T) {
		// Arrange
		slot := &PageSlot{
			Offset: 100,
			Length: 50,
			Flags:  0, // активная запись
		}

		// Act
		data := slot.Serialize()

		// Assert
		require.Len(t, data, SLOT_SIZE)
		require.Equal(t, uint32(100), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(50), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[8:12]))
	})

	t.Run("2. Page slot serialization with deleted record", func(t *testing.T) {
		// Arrange
		slot := &PageSlot{
			Offset: 200,
			Length: 0,
			Flags:  1, // удаленная запись
		}

		// Act
		data := slot.Serialize()

		// Assert
		require.Len(t, data, SLOT_SIZE)
		require.Equal(t, uint32(200), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(1), binary.BigEndian.Uint32(data[8:12]))
	})

	t.Run("3. Page slot serialization with large values", func(t *testing.T) {
		// Arrange
		slot := &PageSlot{
			Offset: 3000,
			Length: 1000,
			Flags:  0,
		}

		// Act
		data := slot.Serialize()

		// Assert
		require.Len(t, data, SLOT_SIZE)
		require.Equal(t, uint32(3000), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(1000), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[8:12]))
	})
}

func TestDeserializePageSlot(t *testing.T) {
	t.Run("1. Page slot deserialization success", func(t *testing.T) {
		// Arrange
		originalSlot := &PageSlot{
			Offset: 150,
			Length: 75,
			Flags:  0,
		}
		data := originalSlot.Serialize()

		// Act
		deserializedSlot, err := (&PageSlot{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.Equal(t, originalSlot.Offset, deserializedSlot.Offset)
		require.Equal(t, originalSlot.Length, deserializedSlot.Length)
		require.Equal(t, originalSlot.Flags, deserializedSlot.Flags)
	})

	t.Run("2. Page slot deserialization with insufficient data", func(t *testing.T) {
		// Arrange
		insufficientData := make([]byte, SLOT_SIZE-1)

		// Act
		slot, err := (&PageSlot{}).Deserialize(insufficientData)

		// Assert
		require.Error(t, err)
		require.Nil(t, slot)
		require.Contains(t, err.Error(), "insufficient data for page slot")
	})

	t.Run("3. Page slot deserialization with empty data", func(t *testing.T) {
		// Arrange
		emptyData := make([]byte, 0)

		// Act
		slot, err := (&PageSlot{}).Deserialize(emptyData)

		// Assert
		require.Error(t, err)
		require.Nil(t, slot)
		require.Contains(t, err.Error(), "insufficient data for page slot")
	})
}

func TestSerializeRawTuple(t *testing.T) {
	t.Run("1. Raw tuple serialization success", func(t *testing.T) {
		// Arrange
		tuple := &RawTuple{
			Length:         20,
			NullBitmapSize: 4,
			NullBitmap:     []byte{0, 1, 0, 1},
			Data:           []byte{1, 2, 3, 4, 5, 6, 7, 8},
		}

		// Act
		data := tuple.Serialize()

		// Assert
		require.Len(t, data, int(tuple.Length))
		require.Equal(t, uint32(20), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(4), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, []byte{0, 1, 0, 1}, data[8:12])
		require.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8}, data[12:20])
	})

	t.Run("2. Raw tuple serialization with empty data", func(t *testing.T) {
		// Arrange
		tuple := &RawTuple{
			Length:         8,
			NullBitmapSize: 0,
			NullBitmap:     []byte{},
			Data:           []byte{},
		}

		// Act
		data := tuple.Serialize()

		// Assert
		require.Len(t, data, int(tuple.Length))
		require.Equal(t, uint32(8), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[4:8]))
	})

	t.Run("3. Raw tuple serialization with large data", func(t *testing.T) {
		// Arrange
		largeData := make([]byte, 992) // Уменьшаем размер данных
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}
		tuple := &RawTuple{
			Length:         1008, // 8 (header) + 8 (null bitmap) + 992 (data)
			NullBitmapSize: 8,
			NullBitmap:     []byte{1, 0, 1, 0, 1, 0, 1, 0},
			Data:           largeData,
		}

		// Act
		data := tuple.Serialize()

		// Assert
		require.Len(t, data, int(tuple.Length))
		require.Equal(t, uint32(1008), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(8), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, []byte{1, 0, 1, 0, 1, 0, 1, 0}, data[8:16])
		require.Equal(t, largeData, data[16:1008])
	})
}

func TestDeserializeRawTuple(t *testing.T) {
	t.Run("1. Raw tuple deserialization success", func(t *testing.T) {
		// Arrange
		originalTuple := &RawTuple{
			Length:         14,        // 4 (Length) + 4 (NullBitmapSize) + 1 (NullBitmap) + 5 (Data) = 14
			NullBitmapSize: 1,         // Для 2 колонок нужен 1 байт
			NullBitmap:     []byte{0}, // 2 бита для 2 колонок
			Data:           []byte{5, 6, 7, 8, 9},
		}
		data := originalTuple.Serialize()

		// Act
		deserializedTuple, err := (&RawTuple{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.Equal(t, originalTuple.Length, deserializedTuple.Length)
		require.Equal(t, originalTuple.NullBitmapSize, deserializedTuple.NullBitmapSize)
		require.Equal(t, originalTuple.NullBitmap, deserializedTuple.NullBitmap)
		require.Equal(t, originalTuple.Data, deserializedTuple.Data)
	})

	t.Run("2. Raw tuple deserialization with insufficient header data", func(t *testing.T) {
		// Arrange
		insufficientData := make([]byte, 7) // Меньше 8 байт для заголовка

		// Act
		tuple, err := (&RawTuple{}).Deserialize(insufficientData)

		// Assert
		require.Error(t, err)
		require.Nil(t, tuple)
		require.Contains(t, err.Error(), "insufficient data for data tuple")
	})

	t.Run("3. Raw tuple deserialization with insufficient total data", func(t *testing.T) {
		// Arrange
		// Создаем данные с заголовком, но недостаточным общим размером
		data := make([]byte, 12)
		binary.BigEndian.PutUint32(data[0:4], 20) // Length = 20
		binary.BigEndian.PutUint32(data[4:8], 4)  // NullBitmapSize = 4
		// Но данных только 4 байта вместо 12

		// Act
		tuple, err := (&RawTuple{}).Deserialize(data)

		// Assert
		require.Error(t, err)
		require.Nil(t, tuple)
		require.Contains(t, err.Error(), "insufficient data for data tuple")
	})

	t.Run("4. Raw tuple deserialization with empty data", func(t *testing.T) {
		// Arrange
		emptyData := make([]byte, 0)

		// Act
		tuple, err := (&RawTuple{}).Deserialize(emptyData)

		// Assert
		require.Error(t, err)
		require.Nil(t, tuple)
		require.Contains(t, err.Error(), "insufficient data for data tuple")
	})
}

func TestNewPage(t *testing.T) {
	t.Run("1. New page creation success", func(t *testing.T) {
		// Arrange
		pageID := PageID{PageNumber: 1}

		// Act
		page := newPage(pageID)

		// Assert
		require.NotNil(t, page)
		require.Equal(t, uint32(1), page.Header.PageID)
		require.Equal(t, uint32(0), page.Header.RecordCount)
		require.Equal(t, uint32(PAGE_HEADER_SIZE), page.Header.Lower)
		require.Equal(t, uint32(PAGE_SIZE), page.Header.Upper)
		require.Len(t, page.Slots, 0)
		require.Len(t, page.RawTuples, 0)
	})

	t.Run("2. New page creation with zero page ID", func(t *testing.T) {
		// Arrange
		pageID := PageID{PageNumber: 0}

		// Act
		page := newPage(pageID)

		// Assert
		require.NotNil(t, page)
		require.Equal(t, uint32(0), page.Header.PageID)
		require.Equal(t, uint32(0), page.Header.RecordCount)
		require.Equal(t, uint32(PAGE_HEADER_SIZE), page.Header.Lower)
		require.Equal(t, uint32(PAGE_SIZE), page.Header.Upper)
		require.Len(t, page.Slots, 0)
		require.Len(t, page.RawTuples, 0)
	})

	t.Run("3. New page creation with large page ID", func(t *testing.T) {
		// Arrange
		pageID := PageID{PageNumber: 9999}

		// Act
		page := newPage(pageID)

		// Assert
		require.NotNil(t, page)
		require.Equal(t, uint32(9999), page.Header.PageID)
		require.Equal(t, uint32(0), page.Header.RecordCount)
		require.Equal(t, uint32(PAGE_HEADER_SIZE), page.Header.Lower)
		require.Equal(t, uint32(PAGE_SIZE), page.Header.Upper)
		require.Len(t, page.Slots, 0)
		require.Len(t, page.RawTuples, 0)
	})
}

func TestSerializePage(t *testing.T) {
	t.Run("1. Page serialization success with empty page", func(t *testing.T) {
		// Arrange
		pageID := PageID{PageNumber: 1}
		page := newPage(pageID)

		// Act
		data := page.Serialize()

		// Assert
		require.Len(t, data, PAGE_SIZE)
		// Проверяем заголовок
		require.Equal(t, uint32(1), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(PAGE_HEADER_SIZE), binary.BigEndian.Uint32(data[8:12]))
		require.Equal(t, uint32(PAGE_SIZE), binary.BigEndian.Uint32(data[12:16]))
	})

	t.Run("2. Page serialization success with records", func(t *testing.T) {
		// Arrange
		pageID := PageID{PageNumber: 2}
		page := newPage(pageID)

		// Добавляем слот
		slot := PageSlot{
			Offset: PAGE_SIZE - 20, // Размещаем с конца страницы
			Length: 20,
			Flags:  0,
		}
		page.Slots = append(page.Slots, slot)
		page.Header.RecordCount = 1
		page.Header.Lower = PAGE_HEADER_SIZE + SLOT_SIZE
		page.Header.Upper = PAGE_SIZE - 20

		// Добавляем tuple
		tuple := RawTuple{
			Length:         20,
			NullBitmapSize: 4,
			NullBitmap:     []byte{0, 1, 0, 1},
			Data:           []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		}
		page.RawTuples = append(page.RawTuples, tuple)

		// Act
		data := page.Serialize()

		// Assert
		require.Len(t, data, PAGE_SIZE)
		// Проверяем заголовок
		require.Equal(t, uint32(2), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(1), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(PAGE_HEADER_SIZE+SLOT_SIZE), binary.BigEndian.Uint32(data[8:12]))
		require.Equal(t, uint32(PAGE_SIZE-20), binary.BigEndian.Uint32(data[12:16]))

		// Проверяем слот
		slotOffset := PAGE_HEADER_SIZE
		require.Equal(t, uint32(PAGE_SIZE-20), binary.BigEndian.Uint32(data[slotOffset:slotOffset+4]))
		require.Equal(t, uint32(20), binary.BigEndian.Uint32(data[slotOffset+4:slotOffset+8]))
		require.Equal(t, uint32(0), binary.BigEndian.Uint32(data[slotOffset+8:slotOffset+12]))
	})

	t.Run("3. Page serialization with multiple records", func(t *testing.T) {
		// Arrange
		pageID := PageID{PageNumber: 3}
		page := newPage(pageID)

		// Добавляем два слота
		slot1 := PageSlot{
			Offset: PAGE_SIZE - 30,
			Length: 15,
			Flags:  0,
		}
		slot2 := PageSlot{
			Offset: PAGE_SIZE - 50,
			Length: 20,
			Flags:  0,
		}
		page.Slots = append(page.Slots, slot1, slot2)
		page.Header.RecordCount = 2
		page.Header.Lower = PAGE_HEADER_SIZE + 2*SLOT_SIZE
		page.Header.Upper = PAGE_SIZE - 50

		// Добавляем tuples
		tuple1 := RawTuple{
			Length:         15,
			NullBitmapSize: 2,
			NullBitmap:     []byte{0, 1},
			Data:           []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
		}
		tuple2 := RawTuple{
			Length:         20,
			NullBitmapSize: 4,
			NullBitmap:     []byte{1, 0, 1, 0},
			Data:           []byte{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160},
		}
		page.RawTuples = append(page.RawTuples, tuple1, tuple2)

		// Act
		data := page.Serialize()

		// Assert
		require.Len(t, data, PAGE_SIZE)
		// Проверяем заголовок
		require.Equal(t, uint32(3), binary.BigEndian.Uint32(data[0:4]))
		require.Equal(t, uint32(2), binary.BigEndian.Uint32(data[4:8]))
		require.Equal(t, uint32(PAGE_HEADER_SIZE+2*SLOT_SIZE), binary.BigEndian.Uint32(data[8:12]))
		require.Equal(t, uint32(PAGE_SIZE-50), binary.BigEndian.Uint32(data[12:16]))
	})
}

func TestDeserializePage(t *testing.T) {
	t.Run("1. Page deserialization success with empty page", func(t *testing.T) {
		// Arrange
		pageID := PageID{PageNumber: 1}
		originalPage := newPage(pageID)
		data := originalPage.Serialize()

		// Act
		deserializedPage, err := (&RawPage{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, deserializedPage)
		require.Equal(t, originalPage.Header.PageID, deserializedPage.Header.PageID)
		require.Equal(t, originalPage.Header.RecordCount, deserializedPage.Header.RecordCount)
		require.Equal(t, originalPage.Header.Lower, deserializedPage.Header.Lower)
		require.Equal(t, originalPage.Header.Upper, deserializedPage.Header.Upper)
		require.Len(t, deserializedPage.Slots, 0)
		require.Len(t, deserializedPage.RawTuples, 0)
	})

	t.Run("2. Page deserialization success with records", func(t *testing.T) {
		// Arrange
		pageID := PageID{PageNumber: 2}
		originalPage := newPage(pageID)

		// Добавляем слот
		slot := PageSlot{
			Offset: PAGE_SIZE - 20,
			Length: 20,
			Flags:  0,
		}
		originalPage.Slots = append(originalPage.Slots, slot)
		originalPage.Header.RecordCount = 1
		originalPage.Header.Lower = PAGE_HEADER_SIZE + SLOT_SIZE
		originalPage.Header.Upper = PAGE_SIZE - 20

		// Добавляем tuple
		tuple := RawTuple{
			Length:         20,
			NullBitmapSize: 4,
			NullBitmap:     []byte{0, 1, 0, 1},
			Data:           []byte{1, 2, 3, 4, 5, 6, 7, 8}, // Уменьшаем размер данных
		}
		originalPage.RawTuples = append(originalPage.RawTuples, tuple)
		data := originalPage.Serialize()

		// Act
		deserializedPage, err := (&RawPage{}).Deserialize(data)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, deserializedPage)
		require.Equal(t, originalPage.Header.PageID, deserializedPage.Header.PageID)
		require.Equal(t, originalPage.Header.RecordCount, deserializedPage.Header.RecordCount)
		require.Equal(t, originalPage.Header.Lower, deserializedPage.Header.Lower)
		require.Equal(t, originalPage.Header.Upper, deserializedPage.Header.Upper)
		require.Len(t, deserializedPage.Slots, 1)
		require.Len(t, deserializedPage.RawTuples, 1)
		require.Equal(t, originalPage.Slots[0].Offset, deserializedPage.Slots[0].Offset)
		require.Equal(t, originalPage.Slots[0].Length, deserializedPage.Slots[0].Length)
		require.Equal(t, originalPage.Slots[0].Flags, deserializedPage.Slots[0].Flags)
		require.Equal(t, originalPage.RawTuples[0].Length, deserializedPage.RawTuples[0].Length)
		require.Equal(t, originalPage.RawTuples[0].NullBitmapSize, deserializedPage.RawTuples[0].NullBitmapSize)
		require.Equal(t, originalPage.RawTuples[0].NullBitmap, deserializedPage.RawTuples[0].NullBitmap)
		require.Equal(t, originalPage.RawTuples[0].Data, deserializedPage.RawTuples[0].Data)
	})

	t.Run("3. Page deserialization with insufficient data", func(t *testing.T) {
		// Arrange
		insufficientData := make([]byte, PAGE_SIZE-1)

		// Act
		page, err := (&RawPage{}).Deserialize(insufficientData)

		// Assert
		require.Error(t, err)
		require.Nil(t, page)
		require.Contains(t, err.Error(), "insufficient data for page")
	})

	t.Run("4. Page deserialization with empty data", func(t *testing.T) {
		// Arrange
		emptyData := make([]byte, 0)

		// Act
		page, err := (&RawPage{}).Deserialize(emptyData)

		// Assert
		require.Error(t, err)
		require.Nil(t, page)
		require.Contains(t, err.Error(), "insufficient data for page")
	})

	t.Run("5. Page deserialization with invalid slot data", func(t *testing.T) {
		// Arrange
		data := make([]byte, PAGE_SIZE)
		// Записываем корректный заголовок
		binary.BigEndian.PutUint32(data[0:4], 1)      // PageID
		binary.BigEndian.PutUint32(data[4:8], 1)      // RecordCount = 1
		binary.BigEndian.PutUint32(data[8:12], 16)    // Lower
		binary.BigEndian.PutUint32(data[12:16], 4000) // Upper
		// Но не записываем слот, что приведет к ошибке при десериализации

		// Act
		page, err := (&RawPage{}).Deserialize(data)

		// Assert
		require.Error(t, err)
		require.Nil(t, page)
		// Ошибка должна возникнуть при десериализации слота
	})
}
