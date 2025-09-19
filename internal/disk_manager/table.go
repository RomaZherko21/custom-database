package disk_manager

import (
	"encoding/binary"
	"fmt"
	"os"
)

// TableSchema представляет схему таблицы
type TableSchema struct {
	FileID      uint32
	ColumnCount uint16
	Columns     []Column
}

// Column представляет колонку таблицы
type Column struct {
	Name       string
	Type       ColumnType // 1=INT, 2=TEXT
	IsNullable bool       // Может ли быть NULL
}

type ColumnType uint16

const (
	ColumnTypeInt  ColumnType = 1
	ColumnTypeText ColumnType = 2
)

const (
	FILE_ID_SIZE      = 4
	COLUMN_COUNT_SIZE = 2

	COLUMN_TYPE_SIZE = 2
)

// Serialize сериализует схему таблицы в байты
func (schema *TableSchema) Serialize() []byte {
	// сериализуем схему таблицы в байты, НЕ В JSON!
	data := []byte{}

	data = append(data, binary.BigEndian.AppendUint32(data, schema.FileID)...)

	return nil
}

func (dm *diskManager) CreateTable(tableName string, schema *TableSchema) error {
	// Проверяем, существует ли таблица в папке tables в корне проекта
	if _, err := os.Stat(fmt.Sprintf("tables/%s.db", tableName)); err == nil {
		return fmt.Errorf("table %s already exists", tableName)
	}

	// Создаем файл таблицы
	metadataFile, err := os.Create(fmt.Sprintf("tables/%s.metadata", tableName))
	if err != nil {
		return fmt.Errorf("failed to create table file: %w", err)
	}
	defer metadataFile.Close()

	// Записываем схему таблицы в файл
	_, err = metadataFile.Write(schema.Serialize())
	if err != nil {
		return fmt.Errorf("failed to write table schema: %w", err)
	}

	return nil
}
