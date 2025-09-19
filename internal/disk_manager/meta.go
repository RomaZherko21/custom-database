package disk_manager

import (
	"encoding/binary"
	"fmt"
)

type MetaFileHeader struct {
	MagicNumber  uint32   // 4 байта - идентификатор мета-файла (0x9ABCDEF0)
	TableNameLen uint32   // 4 байта - длина имени таблицы
	ColumnCount  uint32   // 4 байта - количество колонок
	TableName    [32]byte // 32 байта - имя таблицы (фиксированная длина)
	NextRowID    uint64   // 8 байт - следующий RowID для автоинкремента  (userId int NOT NULL AUTO_INCREMENT)
}

type ColumnInfo struct {
	ColumnName      [16]byte // 16 байт - имя колонки (фиксированная длина)
	DataType        uint8    // 1 байт - тип данных (0=INT, 1=TEXT)
	IsNullable      uint8    // 1 байт - может ли быть NULL (0=no, 1=yes)
	IsPrimaryKey    uint8    // 1 байт - является ли первичным ключом
	IsAutoIncrement uint8    // 1 байт - автоинкремент
	MaxLength       uint16   // 2 байта - максимальная длина (для TEXT)
	DefaultValue    uint32   // 4 байта - значение по умолчанию
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
		TableName:    [32]byte(tableNameBytes),
		NextRowID:    0,
	}
}

// Serialize сериализует мета-файл в байты
func (meta *MetaFileHeader) Serialize() []byte {
	data := make([]byte, 0)

	data = append(data, binary.BigEndian.AppendUint32(data, meta.MagicNumber)...)
	data = append(data, binary.BigEndian.AppendUint32(data, meta.TableNameLen)...)
	data = append(data, binary.BigEndian.AppendUint32(data, meta.ColumnCount)...)
	data = append(data, meta.TableName[:]...)
	data = append(data, binary.BigEndian.AppendUint64(data, meta.NextRowID)...)

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
		TableName:    [32]byte(data[12:44]),
		NextRowID:    binary.BigEndian.Uint64(data[44:52]),
	}, nil
}
