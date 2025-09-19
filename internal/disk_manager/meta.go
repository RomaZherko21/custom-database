package disk_manager

import (
	"encoding/binary"
	"fmt"
	"os"
)

const TABLE_NAME_MAX_LENGTH = 32

// Магическое число для мета-файла, для проверки корректности формата файла
const META_FILE_MAGIC_NUMBER = 0x9ABCDEF0

const META_FILE_HEADER_SIZE = 56

type MetaFileHeader struct {
	MagicNumber  uint32                      // 4 байта - идентификатор мета-файла (0x9ABCDEF0)
	TableNameLen uint32                      // 4 байта - длина имени таблицы
	ColumnCount  uint32                      // 4 байта - количество колонок
	TableName    [TABLE_NAME_MAX_LENGTH]byte // 32 байта - имя таблицы (фиксированная длина)
	NextRowID    uint64                      // 8 байт - следующий RowID для автоинкремента  (userId int NOT NULL AUTO_INCREMENT)
}

func NewMetaFileHeader(tableName string, columnCount uint32) *MetaFileHeader {
	tableNameBytes := []byte(tableName)
	if len(tableNameBytes) > 32 {
		tableNameBytes = tableNameBytes[:32]
	}
	tableNameBytes = append(tableNameBytes, make([]byte, 32-len(tableNameBytes))...)

	return &MetaFileHeader{
		MagicNumber:  META_FILE_MAGIC_NUMBER,
		TableNameLen: uint32(len(tableName)),
		ColumnCount:  columnCount,
		TableName:    [TABLE_NAME_MAX_LENGTH]byte(tableNameBytes),
		NextRowID:    0,
	}
}

// Serialize сериализует мета-файл в байты
func (meta *MetaFileHeader) Serialize() []byte {
	data := make([]byte, 56)

	// Записываем MagicNumber (байты 0-4)
	binary.BigEndian.PutUint32(data[0:4], meta.MagicNumber)

	// Записываем TableNameLen (байты 4-8)
	binary.BigEndian.PutUint32(data[4:8], meta.TableNameLen)

	// Записываем ColumnCount (байты 8-12)
	binary.BigEndian.PutUint32(data[8:12], meta.ColumnCount)

	// Записываем TableName (байты 12-44)
	copy(data[12:12+TABLE_NAME_MAX_LENGTH], meta.TableName[:])

	// Записываем NextRowID (байты 44-52)
	binary.BigEndian.PutUint64(data[12+TABLE_NAME_MAX_LENGTH:12+TABLE_NAME_MAX_LENGTH+8], meta.NextRowID)

	return data
}

func (meta *MetaFileHeader) Deserialize(data []byte) (*MetaFileHeader, error) {
	if len(data) < 56 {
		return nil, fmt.Errorf("insufficient data for meta file header")
	}

	return &MetaFileHeader{
		MagicNumber:  binary.BigEndian.Uint32(data[0:4]),
		TableNameLen: binary.BigEndian.Uint32(data[4:8]),
		ColumnCount:  binary.BigEndian.Uint32(data[8:12]),
		TableName:    [TABLE_NAME_MAX_LENGTH]byte(data[12 : 12+TABLE_NAME_MAX_LENGTH]),
		NextRowID:    binary.BigEndian.Uint64(data[12+TABLE_NAME_MAX_LENGTH : 12+TABLE_NAME_MAX_LENGTH+8]),
	}, nil
}

const COLUMN_NAME_MAX_LENGTH = 32

const COLUMN_INFO_SIZE = 52

type ColumnInfo struct {
	ColumnName      [COLUMN_NAME_MAX_LENGTH]byte // 32 байта - имя колонки (фиксированная длина)
	DataType        DataType                     // 4 байта - тип данных (0=INT, 1=TEXT)
	IsNullable      uint32                       // 4 байта - может ли быть NULL (0=no, 1=yes)
	IsPrimaryKey    uint32                       // 4 байта - является ли первичным ключом
	IsAutoIncrement uint32                       // 4 байта - автоинкремент
	DefaultValue    uint32                       // 4 байта - значение по умолчанию
}

func NewColumnInfo(columnName string, dataType uint32, isNullable uint32, isPrimaryKey uint32, isAutoIncrement uint32, defaultValue uint32) *ColumnInfo {
	columnNameBytes := []byte(columnName)
	if len(columnNameBytes) > 32 {
		columnNameBytes = columnNameBytes[:32]
	}
	columnNameBytes = append(columnNameBytes, make([]byte, 32-len(columnNameBytes))...)

	return &ColumnInfo{
		ColumnName:      [COLUMN_NAME_MAX_LENGTH]byte(columnNameBytes),
		DataType:        DataType(dataType),
		IsNullable:      isNullable,
		IsPrimaryKey:    isPrimaryKey,
		IsAutoIncrement: isAutoIncrement,
		DefaultValue:    defaultValue,
	}
}

// Serialize сериализует ColumnInfo в байты
func (column *ColumnInfo) Serialize() []byte {
	data := make([]byte, 52)

	// Записываем ColumnName (байты 0-32)
	copy(data[0:COLUMN_NAME_MAX_LENGTH], column.ColumnName[:])

	// Записываем DataType (байты 32-36)
	binary.BigEndian.PutUint32(data[COLUMN_NAME_MAX_LENGTH:COLUMN_NAME_MAX_LENGTH+4], uint32(column.DataType))

	// Записываем IsNullable (байты 36-40)
	binary.BigEndian.PutUint32(data[COLUMN_NAME_MAX_LENGTH+4:COLUMN_NAME_MAX_LENGTH+8], column.IsNullable)

	// Записываем IsPrimaryKey (байты 40-44)
	binary.BigEndian.PutUint32(data[COLUMN_NAME_MAX_LENGTH+8:COLUMN_NAME_MAX_LENGTH+12], column.IsPrimaryKey)

	// Записываем IsAutoIncrement (байты 44-48)
	binary.BigEndian.PutUint32(data[COLUMN_NAME_MAX_LENGTH+12:COLUMN_NAME_MAX_LENGTH+16], column.IsAutoIncrement)

	// Записываем DefaultValue (байты 48-52)
	binary.BigEndian.PutUint32(data[COLUMN_NAME_MAX_LENGTH+16:COLUMN_NAME_MAX_LENGTH+20], column.DefaultValue)

	return data
}

func (column *ColumnInfo) Deserialize(data []byte) (*ColumnInfo, error) {
	if len(data) < 52 {
		return nil, fmt.Errorf("insufficient data for column info")
	}

	return &ColumnInfo{
		ColumnName:      [COLUMN_NAME_MAX_LENGTH]byte(data[0:COLUMN_NAME_MAX_LENGTH]),
		DataType:        DataType(binary.BigEndian.Uint32(data[COLUMN_NAME_MAX_LENGTH : COLUMN_NAME_MAX_LENGTH+4])),
		IsNullable:      binary.BigEndian.Uint32(data[COLUMN_NAME_MAX_LENGTH+4 : COLUMN_NAME_MAX_LENGTH+8]),
		IsPrimaryKey:    binary.BigEndian.Uint32(data[COLUMN_NAME_MAX_LENGTH+8 : COLUMN_NAME_MAX_LENGTH+12]),
		IsAutoIncrement: binary.BigEndian.Uint32(data[COLUMN_NAME_MAX_LENGTH+12 : COLUMN_NAME_MAX_LENGTH+16]),
		DefaultValue:    binary.BigEndian.Uint32(data[COLUMN_NAME_MAX_LENGTH+16 : COLUMN_NAME_MAX_LENGTH+20]),
	}, nil
}

type MetaFile struct {
	Header  *MetaFileHeader
	Columns []ColumnInfo
}

func CreateMetaFile(tableName string, columns []ColumnInfo) (*MetaFile, error) {
	// Проверяем, существует ли таблица в папке tables в корне проекта
	if _, err := os.Stat(fmt.Sprintf("tables/%s.meta", tableName)); err == nil {
		return nil, fmt.Errorf("table %s already exists", tableName)
	}

	// Создаем .meta файл в папке tables
	metaFilePath := fmt.Sprintf("tables/%s.meta", tableName)
	metaFile, err := os.Create(metaFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create meta file: %w", err)
	}
	defer metaFile.Close()

	// Записываем заголовок в мета-файл
	header := NewMetaFileHeader(tableName, uint32(len(columns)))
	_, err = metaFile.Write(header.Serialize())
	if err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	// Записываем колонки в мета-файл
	for _, column := range columns {
		_, err = metaFile.Write(column.Serialize())
		if err != nil {
			return nil, fmt.Errorf("failed to write column: %w", err)
		}
	}

	return &MetaFile{
		Header:  header,
		Columns: columns,
	}, nil
}

func ReadMetaFile(tableName string) (*MetaFile, error) {
	// Проверяем, существует ли таблица в папке tables в корне проекта
	if _, err := os.Stat(fmt.Sprintf("tables/%s.meta", tableName)); err != nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	// Читаем мета-файл
	metaFile, err := os.Open(fmt.Sprintf("tables/%s.meta", tableName))
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

	// Десериализуем заголовок
	header, err := (&MetaFileHeader{}).Deserialize(headerBytes)
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

	return &MetaFile{
		Header:  header,
		Columns: columns,
	}, nil
}
