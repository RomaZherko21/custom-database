package disk_manager

import (
	"encoding/binary"
	"fmt"
)

// DataType представляет тип данных колонки
type DataType uint32

const (
	// INT_32_TYPE - 32-битное целое число (4 байта)
	INT_32_TYPE DataType = 1
	// TEXT_TYPE - строка переменной длины (4 байта длины + данные)
	TEXT_TYPE DataType = 2
)

// Page представляет страницу данных для buffer pool
type Page struct {
	Header  PageHeader   // Заголовок страницы
	Slots   []PageSlot   // Слоты для указателей на записи
	Rows    []Row        // Записи данных
	Columns []ColumnInfo // Схема колонок
}

// FileID представляет идентификатор файла таблицы
type FileID struct {
	FileID uint32 // ID файла
}

// PageID представляет идентификатор страницы
type PageID struct {
	PageNumber uint32 // Номер страницы в файле
}

// RowID представляет идентификатор строки
type RowID struct {
	PageID     uint32 // Номер страницы в файле
	SlotNumber uint32 // Номер слота в странице
}

// Row представляет строку данных как массив ячеек
type Row []DataCell

// GetSize возвращает общий размер строки в байтах
func (r Row) GetSize() uint32 {
	size := uint32(0)
	for _, cell := range r {
		size += cell.GetSize()
	}

	// Добавляем размер заголовка строки: длина + размер NullBitmap + NullBitmap
	nullBitmapSize, err := CalculateNullBitmapSize(len(r))
	if err != nil {
		nullBitmapSize = 1 // fallback
	}

	size += TUPLE_LENGTH_FIELD_SIZE + TUPLE_NULL_BITMAP_SIZE + nullBitmapSize
	return size
}

// DataCell представляет ячейку данных
type DataCell struct {
	DataType DataType    // Тип данных
	Data     interface{} // Значение данных
	IsNull   bool        // Флаг NULL значения
}

// SerializeData сериализует данные ячейки в байты
func (cell *DataCell) SerializeData() []byte {
	if cell.IsNull {
		return []byte{}
	}

	switch cell.DataType {
	case INT_32_TYPE:
		return cell.serializeInt32()
	case TEXT_TYPE:
		return cell.serializeText()
	default:
		return []byte{}
	}
}

// serializeInt32 сериализует int32 в 4 байта
func (cell *DataCell) serializeInt32() []byte {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, uint32(cell.Data.(int32)))
	return data
}

// deserializeInt32 десериализует int32 из байтов
func (cell *DataCell) deserializeInt32(data []byte) (*DataCell, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("insufficient data for INT_32_TYPE: need 4 bytes, got %d", len(data))
	}
	cell.Data = int32(binary.BigEndian.Uint32(data[0:4]))
	return cell, nil
}

// serializeText сериализует строку: 4 байта длины + данные
func (cell *DataCell) serializeText() []byte {
	str := cell.Data.(string)
	length := uint32(len(str))

	data := make([]byte, 4+length)
	binary.BigEndian.PutUint32(data[0:4], length)
	copy(data[4:], str)
	return data
}

// deserializeText десериализует строку из байтов
func (cell *DataCell) deserializeText(data []byte) (*DataCell, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("insufficient data for TEXT_TYPE: need at least 4 bytes for length, got %d", len(data))
	}

	// Десериализуем длину строки
	length := binary.BigEndian.Uint32(data[0:4])
	if len(data) < int(4+length) {
		return nil, fmt.Errorf("insufficient data for TEXT_TYPE: need %d bytes, got %d", 4+length, len(data))
	}

	// Десериализуем строку
	cell.Data = string(data[4 : 4+length])
	return cell, nil
}

// GetSize возвращает размер данных ячейки в байтах
func (cell *DataCell) GetSize() uint32 {
	if cell.IsNull {
		return 0
	}

	switch cell.DataType {
	case INT_32_TYPE:
		return 4
	case TEXT_TYPE:
		return 4 + uint32(len(cell.Data.(string)))
	default:
		return 0
	}
}

// DeserializeDataCell десериализует данные ячейки из байтов
func DeserializeDataCell(data []byte, dataType DataType, isNull bool) (*DataCell, error) {
	cell := &DataCell{
		DataType: dataType,
		IsNull:   isNull,
	}

	if isNull {
		cell.Data = nil
		return cell, nil
	}

	switch dataType {
	case INT_32_TYPE:
		return cell.deserializeInt32(data)
	case TEXT_TYPE:
		return cell.deserializeText(data)
	default:
		return nil, fmt.Errorf("unsupported data type: %d", dataType)
	}
}

// ConvertRawTupleToRow конвертирует RawTuple в Row
func ConvertRawTupleToRow(rawTuple RawTuple, columns []ColumnInfo) (Row, error) {
	row := make(Row, 0, len(columns))
	dataOffset := uint32(0)

	for i, column := range columns {
		// Определяем, является ли значение NULL по биту в NullBitmap
		isNull := false
		if i < len(columns) {
			byteIndex := i / 8
			bitIndex := i % 8
			if byteIndex < len(rawTuple.NullBitmap) {
				isNull = (rawTuple.NullBitmap[byteIndex] & (1 << bitIndex)) != 0
			}
		}

		var cell *DataCell
		var err error

		if isNull {
			cell = &DataCell{
				DataType: column.DataType,
				IsNull:   true,
				Data:     nil,
			}
		} else {
			// Определяем размер данных для текущей колонки
			var cellDataSize uint32
			switch column.DataType {
			case INT_32_TYPE:
				cellDataSize = 4
			case TEXT_TYPE:
				if dataOffset+4 > uint32(len(rawTuple.Data)) {
					return nil, fmt.Errorf("insufficient data for TEXT_TYPE length field at offset %d", dataOffset)
				}
				textLength := binary.BigEndian.Uint32(rawTuple.Data[dataOffset : dataOffset+4])
				cellDataSize = 4 + textLength
			default:
				return nil, fmt.Errorf("unsupported data type: %d", column.DataType)
			}

			// Проверяем, что у нас достаточно данных
			if dataOffset+cellDataSize > uint32(len(rawTuple.Data)) {
				return nil, fmt.Errorf("insufficient data for column %d at offset %d", i, dataOffset)
			}

			// Извлекаем данные для текущей колонки
			cellData := rawTuple.Data[dataOffset : dataOffset+cellDataSize]
			cell, err = DeserializeDataCell(cellData, column.DataType, false)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize cell %d: %w", i, err)
			}

			dataOffset += cellDataSize
		}

		row = append(row, *cell)
	}

	return row, nil
}

// ConvertRowToRawTuple конвертирует Row в RawTuple
func ConvertRowToRawTuple(row Row) *RawTuple {
	columnCount := len(row)

	// Вычисляем правильный размер NullBitmap
	var nullBitmapSize uint32
	if columnCount == 0 {
		// Для пустой строки используем минимальный размер
		nullBitmapSize = 1
	} else {
		var err error
		nullBitmapSize, err = CalculateNullBitmapSize(columnCount)
		if err != nil {
			// Если ошибка, используем минимальный размер
			nullBitmapSize = 1
		}
	}

	// Создаем NullBitmap правильного размера
	nullBitmap := make([]byte, nullBitmapSize)

	// Заполняем NullBitmap битами
	for i, cell := range row {
		if cell.IsNull {
			byteIndex := i / 8
			bitIndex := i % 8
			nullBitmap[byteIndex] |= (1 << bitIndex)
		}
	}

	data := make([]byte, 0, row.GetSize())
	for _, cell := range row {
		data = append(data, cell.SerializeData()...)
	}

	// Вычисляем общую длину tuple
	totalLength := TUPLE_LENGTH_FIELD_SIZE + TUPLE_NULL_BITMAP_SIZE + nullBitmapSize + uint32(len(data))

	return &RawTuple{
		Length:         totalLength,
		NullBitmapSize: nullBitmapSize,
		NullBitmap:     nullBitmap,
		Data:           data,
	}
}

// ConvertPageToRawPage конвертирует Page в RawPage
func ConvertPageToRawPage(page *Page) *RawPage {
	rawTuples := make([]RawTuple, 0, len(page.Rows))
	for _, row := range page.Rows {
		rawTuples = append(rawTuples, *ConvertRowToRawTuple(row))
	}

	return &RawPage{
		Header:    page.Header,
		Slots:     page.Slots,
		RawTuples: rawTuples,
	}
}

// CalculateNullBitmapSize вычисляет размер NullBitmap в байтах на основе количества колонок
// 1-8 колонок = 1 байт, 9-16 колонок = 2 байта, 17-24 колонки = 3 байта, 25-32 колонки = 4 байта
func CalculateNullBitmapSize(columnCount int) (uint32, error) {
	if columnCount <= 0 {
		return 0, fmt.Errorf("column count must be positive")
	}
	if columnCount > MAX_TABLE_COLUMNS_AMOUNT {
		return 0, fmt.Errorf("column count exceeds maximum allowed (%d)", MAX_TABLE_COLUMNS_AMOUNT)
	}

	// Вычисляем количество байтов: ceil(columnCount / 8)
	bytes := (columnCount + 7) / 8
	return uint32(bytes), nil
}
