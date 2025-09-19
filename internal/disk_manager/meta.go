package disk_manager

import (
	"encoding/binary"
	"fmt"
)

const TABLE_NAME_MAX_LENGTH = 32

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
		MagicNumber:  0x9ABCDEF0,
		TableNameLen: uint32(len(tableName)),
		ColumnCount:  columnCount,
		TableName:    [TABLE_NAME_MAX_LENGTH]byte(tableNameBytes),
		NextRowID:    0,
	}
}

// Serialize сериализует мета-файл в байты
func (meta *MetaFileHeader) Serialize() []byte {
	data := make([]byte, 56)

	binary.BigEndian.PutUint32(data[0:4], meta.MagicNumber)
	binary.BigEndian.PutUint32(data[4:8], meta.TableNameLen)
	binary.BigEndian.PutUint32(data[8:12], meta.ColumnCount)
	copy(data[12:12+TABLE_NAME_MAX_LENGTH], meta.TableName[:])
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

type ColumnInfo struct {
	ColumnName      [COLUMN_NAME_MAX_LENGTH]byte // 16 байт - имя колонки (фиксированная длина)
	DataType        DataType                     // 4 байт - тип данных (0=INT, 1=TEXT)
	IsNullable      uint32                       // 4 байт - может ли быть NULL (0=no, 1=yes)
	IsPrimaryKey    uint32                       // 4 байт - является ли первичным ключом
	IsAutoIncrement uint32                       // 4 байт - автоинкремент
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

// Serialize сериализует мета-файл в байты
func (column *ColumnInfo) Serialize() []byte {
	data := make([]byte, 28)

	copy(data[0:COLUMN_NAME_MAX_LENGTH], column.ColumnName[:])
	binary.BigEndian.PutUint32(data[COLUMN_NAME_MAX_LENGTH:COLUMN_NAME_MAX_LENGTH+4], uint32(column.DataType))
	binary.BigEndian.PutUint32(data[COLUMN_NAME_MAX_LENGTH+4:COLUMN_NAME_MAX_LENGTH+8], uint32(column.IsNullable))
	binary.BigEndian.PutUint32(data[COLUMN_NAME_MAX_LENGTH+8:COLUMN_NAME_MAX_LENGTH+12], uint32(column.IsPrimaryKey))
	binary.BigEndian.PutUint32(data[COLUMN_NAME_MAX_LENGTH+12:COLUMN_NAME_MAX_LENGTH+16], uint32(column.IsAutoIncrement))
	binary.BigEndian.PutUint32(data[COLUMN_NAME_MAX_LENGTH+20:COLUMN_NAME_MAX_LENGTH+24], uint32(column.DefaultValue))

	return data
}

func (column *ColumnInfo) Deserialize(data []byte) (*ColumnInfo, error) {
	if len(data) < 28 {
		return nil, fmt.Errorf("insufficient data for column info")
	}

	return &ColumnInfo{
		ColumnName:      [COLUMN_NAME_MAX_LENGTH]byte(data[0:COLUMN_NAME_MAX_LENGTH]),
		DataType:        DataType(binary.BigEndian.Uint32(data[COLUMN_NAME_MAX_LENGTH : COLUMN_NAME_MAX_LENGTH+4])),
		IsNullable:      binary.BigEndian.Uint32(data[COLUMN_NAME_MAX_LENGTH+4 : COLUMN_NAME_MAX_LENGTH+8]),
		IsPrimaryKey:    binary.BigEndian.Uint32(data[COLUMN_NAME_MAX_LENGTH+8 : COLUMN_NAME_MAX_LENGTH+12]),
		IsAutoIncrement: binary.BigEndian.Uint32(data[COLUMN_NAME_MAX_LENGTH+12 : COLUMN_NAME_MAX_LENGTH+16]),
		DefaultValue:    binary.BigEndian.Uint32(data[COLUMN_NAME_MAX_LENGTH+20 : COLUMN_NAME_MAX_LENGTH+24]),
	}, nil
}
