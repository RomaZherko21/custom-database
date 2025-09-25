package disk_manager

import (
	"encoding/binary"
	"fmt"
	"os"
)

const META_FILE_HEADER_SIZE = 52 // 4 + 4 + 4 + 32 + 8 = 52 байта (с фиксированным TableName)

type MetaDataHeader struct {
	MagicNumber  uint32 // 4 байта - идентификатор мета-файла
	TableNameLen uint32 // 4 байта - длина имени таблицы
	ColumnCount  uint32 // 4 байта - количество колонок
	TableName    string // строка до 32 байт (сериализуется как фиксированные 32 байта)
	NextRowID    uint64 // 8 байт - следующий RowID для автоинкремента
}

func newMetaFileHeader(tableName string, columnCount uint32) *MetaDataHeader {
	// Проверяем, что имя таблицы не превышает максимальную длину
	if len(tableName) > TABLE_NAME_MAX_LENGTH {
		tableName = tableName[:TABLE_NAME_MAX_LENGTH]
	}

	return &MetaDataHeader{
		MagicNumber:  META_FILE_MAGIC_NUMBER,
		TableNameLen: uint32(len(tableName)),
		ColumnCount:  columnCount,
		NextRowID:    0,
		TableName:    tableName,
	}
}

// Serialize сериализует мета-файл в байты
func (meta *MetaDataHeader) Serialize() []byte {
	data := make([]byte, META_FILE_HEADER_SIZE)

	// Записываем MagicNumber (байты 0-4)
	binary.BigEndian.PutUint32(data[0:4], meta.MagicNumber)

	// Записываем TableNameLen (байты 4-8)
	binary.BigEndian.PutUint32(data[4:8], meta.TableNameLen)

	// Записываем ColumnCount (байты 8-12)
	binary.BigEndian.PutUint32(data[8:12], meta.ColumnCount)

	// Записываем TableName (байты 12-44) - полные 32 байта с нулевым заполнением
	tableNameBytes := []byte(meta.TableName)
	copy(data[12:12+len(tableNameBytes)], tableNameBytes)
	// Остальные байты уже заполнены нулями благодаря make([]byte, META_FILE_HEADER_SIZE)

	// Записываем NextRowID (байты 44-52)
	binary.BigEndian.PutUint64(data[12+TABLE_NAME_MAX_LENGTH:12+TABLE_NAME_MAX_LENGTH+8], meta.NextRowID)

	return data
}

func (meta *MetaDataHeader) Deserialize(data []byte) (*MetaDataHeader, error) {
	if len(data) < META_FILE_HEADER_SIZE {
		return nil, fmt.Errorf("insufficient data for meta file header")
	}

	// Читаем фиксированную часть заголовка
	magicNumber := binary.BigEndian.Uint32(data[0:4])
	tableNameLen := binary.BigEndian.Uint32(data[4:8])
	columnCount := binary.BigEndian.Uint32(data[8:12])
	nextRowID := binary.BigEndian.Uint64(data[12+TABLE_NAME_MAX_LENGTH : 12+TABLE_NAME_MAX_LENGTH+8])

	// Читаем TableName - только значимые байты до tableNameLen
	tableNameBytes := data[12 : 12+tableNameLen]
	tableName := string(tableNameBytes)

	return &MetaDataHeader{
		MagicNumber:  magicNumber,
		TableNameLen: tableNameLen,
		ColumnCount:  columnCount,
		NextRowID:    nextRowID,
		TableName:    tableName,
	}, nil
}

const COLUMN_INFO_SIZE = 56 // 4 + 4 + 4 + 4 + 4 + 4 + 32 = 56 байт (с фиксированным ColumnName)

type ColumnInfo struct {
	ColumnNameLength uint32   // 4 байта - длина имени колонки
	DataType         DataType // 4 байта - тип данных (0=INT, 1=TEXT)
	IsNullable       uint32   // 4 байта - может ли быть NULL (0=no, 1=yes)
	IsPrimaryKey     uint32   // 4 байта - является ли первичным ключом
	IsAutoIncrement  uint32   // 4 байта - автоинкремент
	DefaultValue     uint32   // 4 байта - значение по умолчанию
	ColumnName       string   // строка до 32 байт (сериализуется как фиксированные 32 байта)
}

// Serialize сериализует ColumnInfo в байты
func (column *ColumnInfo) Serialize() []byte {
	data := make([]byte, COLUMN_INFO_SIZE)

	// Записываем ColumnNameLength (байты 0-4)
	actualLength := uint32(len(column.ColumnName))
	if actualLength > COLUMN_NAME_MAX_LENGTH {
		actualLength = COLUMN_NAME_MAX_LENGTH
	}
	binary.BigEndian.PutUint32(data[0:4], actualLength)

	// Записываем DataType (байты 4-8)
	binary.BigEndian.PutUint32(data[4:8], uint32(column.DataType))

	// Записываем IsNullable (байты 8-12)
	binary.BigEndian.PutUint32(data[8:12], column.IsNullable)

	// Записываем IsPrimaryKey (байты 12-16)
	binary.BigEndian.PutUint32(data[12:16], column.IsPrimaryKey)

	// Записываем IsAutoIncrement (байты 16-20)
	binary.BigEndian.PutUint32(data[16:20], column.IsAutoIncrement)

	// Записываем DefaultValue (байты 20-24)
	binary.BigEndian.PutUint32(data[20:24], column.DefaultValue)

	// Записываем ColumnName (байты 24-56) - полные 32 байта с нулевым заполнением
	columnNameBytes := []byte(column.ColumnName)
	// Ограничиваем длину до максимальной
	if len(columnNameBytes) > COLUMN_NAME_MAX_LENGTH {
		columnNameBytes = columnNameBytes[:COLUMN_NAME_MAX_LENGTH]
	}
	// Копируем только доступную часть
	copyLength := len(columnNameBytes)
	if copyLength > COLUMN_NAME_MAX_LENGTH {
		copyLength = COLUMN_NAME_MAX_LENGTH
	}
	// Проверяем, что не выходим за границы массива
	if 24+copyLength > COLUMN_INFO_SIZE {
		copyLength = COLUMN_INFO_SIZE - 24
	}
	copy(data[24:24+copyLength], columnNameBytes)
	// Остальные байты уже заполнены нулями благодаря make([]byte, COLUMN_INFO_SIZE)

	return data
}

func (column *ColumnInfo) Deserialize(data []byte) (*ColumnInfo, error) {
	if len(data) < COLUMN_INFO_SIZE {
		return nil, fmt.Errorf("insufficient data for column info")
	}

	// Читаем фиксированную часть
	columnNameLength := binary.BigEndian.Uint32(data[0:4])
	dataType := DataType(binary.BigEndian.Uint32(data[4:8]))
	isNullable := binary.BigEndian.Uint32(data[8:12])
	isPrimaryKey := binary.BigEndian.Uint32(data[12:16])
	isAutoIncrement := binary.BigEndian.Uint32(data[16:20])
	defaultValue := binary.BigEndian.Uint32(data[20:24])

	// Читаем ColumnName - только значимые байты до columnNameLength
	columnNameBytes := data[24 : 24+columnNameLength]
	columnName := string(columnNameBytes)

	return &ColumnInfo{
		ColumnNameLength: columnNameLength,
		DataType:         dataType,
		IsNullable:       isNullable,
		IsPrimaryKey:     isPrimaryKey,
		IsAutoIncrement:  isAutoIncrement,
		DefaultValue:     defaultValue,
		ColumnName:       columnName,
	}, nil
}

type MetaData struct {
	Header  *MetaDataHeader
	Columns []ColumnInfo
}

func createMetaFile(tableName string, columns []ColumnInfo) (*MetaData, error) {
	metaFilePath := fmt.Sprintf(META_FILE_PATH, tableName)

	// Проверяем, существует ли таблица в папке tables в корне проекта
	if _, err := os.Stat(metaFilePath); err == nil {
		return nil, fmt.Errorf("table %s already exists", tableName)
	}

	// Создаем .meta файл в папке tables
	metaFile, err := os.Create(metaFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create meta file: %w", err)
	}
	defer metaFile.Close()

	// Записываем заголовок в мета-файл
	header := newMetaFileHeader(tableName, uint32(len(columns)))
	_, err = metaFile.Write(header.Serialize())
	if err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	// Записываем колонки в мета-файл
	for _, column := range columns {
		// Устанавливаем ColumnNameLength перед сериализацией
		column.ColumnNameLength = uint32(len(column.ColumnName))
		_, err = metaFile.Write(column.Serialize())
		if err != nil {
			return nil, fmt.Errorf("failed to write column: %w", err)
		}
	}

	return &MetaData{
		Header:  header,
		Columns: columns,
	}, nil
}

func readMetaFile(tableName string) (*MetaData, error) {
	metaFilePath := fmt.Sprintf(META_FILE_PATH, tableName)

	// Проверяем, существует ли таблица в папке tables в корне проекта
	if _, err := os.Stat(metaFilePath); err != nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	// Читаем мета-файл
	metaFile, err := os.Open(metaFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open meta file: %w", err)
	}
	defer metaFile.Close()

	// Читаем заголовок
	headerBytes := make([]byte, META_FILE_HEADER_SIZE)
	n, err := metaFile.Read(headerBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}
	if n != META_FILE_HEADER_SIZE {
		return nil, fmt.Errorf("incomplete header read: got %d bytes, expected %d", n, META_FILE_HEADER_SIZE)
	}
	// проверяем на magic number
	if binary.BigEndian.Uint32(headerBytes[0:4]) != META_FILE_MAGIC_NUMBER {
		return nil, fmt.Errorf("invalid magic number")
	}

	// Десериализуем заголовок
	header, err := (&MetaDataHeader{}).Deserialize(headerBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize header: %w", err)
	}

	// Читаем колонки
	columns := make([]ColumnInfo, header.ColumnCount)
	for i := 0; i < int(header.ColumnCount); i++ {
		columnBytes := make([]byte, COLUMN_INFO_SIZE)

		n, err := metaFile.Read(columnBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to read column %d: %w", i, err)
		}
		if n != COLUMN_INFO_SIZE {
			return nil, fmt.Errorf("incomplete column %d read: got %d bytes, expected %d", i, n, COLUMN_INFO_SIZE)
		}

		// Десериализуем колонку
		column, err := (&ColumnInfo{}).Deserialize(columnBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize column %d: %w", i, err)
		}
		columns[i] = *column
	}

	return &MetaData{
		Header:  header,
		Columns: columns,
	}, nil
}

func writeMetaFile(tableName string, metaData *MetaData) (*MetaData, error) {
	metaFilePath := fmt.Sprintf(META_FILE_PATH, tableName)

	// Проверяем, существует ли таблица в папке tables в корне проекта
	if _, err := os.Stat(metaFilePath); err != nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	// Создаем пустой массив байт
	data := make([]byte, 0)

	// Записываем заголовок
	data = append(data, metaData.Header.Serialize()...)

	// Записываем колонки
	for _, column := range metaData.Columns {
		// Устанавливаем ColumnNameLength перед сериализацией
		column.ColumnNameLength = uint32(len(column.ColumnName))
		data = append(data, column.Serialize()...)
	}

	// Открываем файл для записи
	file, err := os.OpenFile(metaFilePath, os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open meta file: %w", err)
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return nil, fmt.Errorf("failed to write data: %w", err)
	}

	return metaData, nil
}

func deleteMetaFile(tableName string) error {
	metaFilePath := fmt.Sprintf(META_FILE_PATH, tableName)

	// Проверяем, существует ли таблица в папке tables в корне проекта
	if _, err := os.Stat(metaFilePath); err != nil {
		return fmt.Errorf("table %s not found", tableName)
	}

	// Удаляем .meta файл в папке tables в корне проекта
	return os.Remove(metaFilePath)
}
