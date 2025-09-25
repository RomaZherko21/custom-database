package disk_manager

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

// TablesListHeader представляет заголовок файла списка таблиц
type TablesListHeader struct {
	MagicNumber uint32 // 4 байта - идентификатор файла списка таблиц
}

// Размер заголовка файла списка таблиц
const TABLES_LIST_HEADER_SIZE = 4

// Serialize сериализует TablesListHeader в байты
func (header *TablesListHeader) Serialize() []byte {
	data := make([]byte, TABLES_LIST_HEADER_SIZE)

	// Записываем MagicNumber (байты 0-4)
	binary.BigEndian.PutUint32(data[0:4], header.MagicNumber)

	return data
}

// Deserialize десериализует TablesListHeader из байтов
func (header *TablesListHeader) Deserialize(data []byte) (*TablesListHeader, error) {
	if len(data) < TABLES_LIST_HEADER_SIZE {
		return nil, fmt.Errorf("insufficient data for tables list header")
	}

	return &TablesListHeader{
		MagicNumber: binary.BigEndian.Uint32(data[0:4]),
	}, nil
}

// TableEntry представляет запись о таблице в списке
type TableEntry struct {
	TableNameLength uint32                      // 4 байта - длина имени таблицы
	TableName       [TABLE_NAME_MAX_LENGTH]byte // 32 байта - имя таблицы
	FileID          FileID                      // 4 байта - идентификатор файла таблицы
}

// Размер записи о таблице в списке таблиц
const TABLES_LIST_ENTRY_SIZE = 40

// Serialize сериализует TableEntry в байты
func (entry *TableEntry) Serialize() []byte {
	data := make([]byte, TABLES_LIST_ENTRY_SIZE)

	// Записываем TableNameLength (байты 0-4)
	binary.BigEndian.PutUint32(data[0:4], entry.TableNameLength)

	// Записываем TableName только нужное количество байтов (байты 4-4+tableNameLength)
	copy(data[4:4+entry.TableNameLength], entry.TableName[:entry.TableNameLength])

	// Записываем FileID (байты 36-40)
	binary.BigEndian.PutUint32(data[36:40], entry.FileID.FileID)

	return data
}

// Deserialize десериализует TableEntry из байтов
func (entry *TableEntry) Deserialize(data []byte) (*TableEntry, error) {
	if len(data) < TABLES_LIST_ENTRY_SIZE {
		return nil, fmt.Errorf("insufficient data for table entry")
	}

	tableNameLength := binary.BigEndian.Uint32(data[0:4])
	if tableNameLength > TABLE_NAME_MAX_LENGTH {
		return nil, fmt.Errorf("table name length exceeds maximum")
	}

	// Извлекаем имя таблицы с учетом его реальной длины
	tableNameBytes := make([]byte, tableNameLength)
	copy(tableNameBytes, data[4:4+tableNameLength])

	// Создаем массив байтов для TableName
	var tableName [TABLE_NAME_MAX_LENGTH]byte
	copy(tableName[:], tableNameBytes)

	return &TableEntry{
		TableNameLength: tableNameLength,
		TableName:       tableName,
		FileID:          FileID{FileID: binary.BigEndian.Uint32(data[36:40])},
	}, nil
}

// TablesList представляет список всех таблиц в системе
type TablesList struct {
	Header *TablesListHeader // Заголовок файла
	Tables map[string]FileID // Карта: имя таблицы -> FileID
}

// NewTablesList создает новый пустой список таблиц
func NewTablesList() *TablesList {
	return &TablesList{
		Header: &TablesListHeader{
			MagicNumber: TABLES_LIST_MAGIC_NUMBER,
		},
		Tables: make(map[string]FileID),
	}
}

// Serialize сериализует весь список таблиц в байты
func (tl *TablesList) Serialize() []byte {
	// Вычисляем размер данных
	entryCount := len(tl.Tables)
	data := make([]byte, TABLES_LIST_HEADER_SIZE+entryCount*TABLES_LIST_ENTRY_SIZE)

	// Записываем заголовок
	headerData := tl.Header.Serialize()
	copy(data[0:TABLES_LIST_HEADER_SIZE], headerData)

	// Записываем записи таблиц
	offset := TABLES_LIST_HEADER_SIZE
	for tableName, fileID := range tl.Tables {
		// Создаем массив байтов для TableName
		var tableNameBytes [TABLE_NAME_MAX_LENGTH]byte
		copy(tableNameBytes[:], []byte(tableName))

		entry := &TableEntry{
			TableNameLength: uint32(len(tableName)),
			TableName:       tableNameBytes,
			FileID:          fileID,
		}

		// Копируем запись в массив байтов
		copy(data[offset:offset+TABLES_LIST_ENTRY_SIZE], entry.Serialize())
		offset += TABLES_LIST_ENTRY_SIZE
	}

	return data
}

// Deserialize десериализует список таблиц из байтов
func (tl *TablesList) Deserialize(data []byte) error {
	if len(data) < TABLES_LIST_HEADER_SIZE {
		return fmt.Errorf("insufficient data for table list")
	}

	// Десериализуем заголовок
	header, err := (&TablesListHeader{}).Deserialize(data[0:TABLES_LIST_HEADER_SIZE])
	if err != nil {
		return fmt.Errorf("failed to deserialize table list header: %w", err)
	}

	// Проверяем magic number
	if header.MagicNumber != TABLES_LIST_MAGIC_NUMBER {
		return fmt.Errorf("invalid table list magic number: expected 0x%X, got 0x%X", TABLES_LIST_MAGIC_NUMBER, header.MagicNumber)
	}

	tl.Header = header
	tl.Tables = make(map[string]FileID)

	// Вычисляем количество записей
	remainingData := data[TABLES_LIST_HEADER_SIZE:]
	entryCount := len(remainingData) / TABLES_LIST_ENTRY_SIZE

	// Десериализуем записи таблиц
	for i := 0; i < entryCount; i++ {
		offset := i * TABLES_LIST_ENTRY_SIZE
		entryData := remainingData[offset : offset+TABLES_LIST_ENTRY_SIZE]

		entry, err := (&TableEntry{}).Deserialize(entryData)
		if err != nil {
			return fmt.Errorf("failed to deserialize entry %d: %w", i, err)
		}

		// Извлекаем имя таблицы
		tableName := string(entry.TableName[:entry.TableNameLength])
		tl.Tables[tableName] = entry.FileID
	}

	return nil
}

// createTableListFile создает файл списка таблиц только с заголовком
// Нужен при создании базы данных, но таблиц еще нет
func createTableListFile() (*TablesList, error) {
	if _, err := os.Stat(TABLE_LIST_FILE_PATH); err == nil {
		return nil, fmt.Errorf("tables list file already exists")
	}

	// Создаем директорию для файла
	err := os.MkdirAll(filepath.Dir(TABLE_LIST_FILE_PATH), 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create tables list directory: %w", err)
	}

	tableList := NewTablesList()
	err = os.WriteFile(TABLE_LIST_FILE_PATH, tableList.Header.Serialize(), 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to write tables list file: %w", err)
	}

	return tableList, nil
}

// readTableListFile читает файл списка таблиц
func readTableListFile() (*TablesList, error) {
	// Проверяем, существует ли файл
	if _, err := os.Stat(TABLE_LIST_FILE_PATH); os.IsNotExist(err) {
		return nil, err // Возвращаем оригинальную ошибку os.IsNotExist
	}

	// Читаем файл
	data, err := os.ReadFile(TABLE_LIST_FILE_PATH)
	if err != nil {
		return nil, fmt.Errorf("failed to read tables list file: %w", err)
	}

	// Десериализуем данные
	tablesList := &TablesList{}
	err = tablesList.Deserialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize tables list: %w", err)
	}

	return tablesList, nil
}

// writeTablesListFile записывает список таблиц в файл
func writeTablesListFile(tablesList *TablesList) (*TablesList, error) {
	// Сериализуем данные
	data := tablesList.Serialize()

	// Записываем в файл
	err := os.WriteFile(TABLE_LIST_FILE_PATH, data, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to write tables list file: %w", err)
	}

	return tablesList, nil
}

// addTableInList обновляет список таблиц после создания новой таблицы
func addTableInList(tableName string) (*TablesList, error) {
	// Пытаемся прочитать существующий список таблиц
	tablesList, err := readTableListFile()
	if err != nil {
		return nil, fmt.Errorf("failed to read tables list: %w", err)
	}

	// Генерируем новый FileID для таблицы
	// Используем простую логику: следующий ID = количество таблиц + 1
	nextFileID := uint32(len(tablesList.Tables) + 1)
	fileID := FileID{FileID: nextFileID}

	// Добавляем таблицу в список
	if len(tableName) > TABLE_NAME_MAX_LENGTH {
		return nil, fmt.Errorf("table name too long: %d bytes, maximum %d", len(tableName), TABLE_NAME_MAX_LENGTH)
	}
	tablesList.Tables[tableName] = fileID

	// Записываем обновленный список
	tablesList, err = writeTablesListFile(tablesList)
	if err != nil {
		return nil, fmt.Errorf("failed to write tables list: %w", err)
	}

	return tablesList, nil
}

// deleteTableInList обновляет список таблиц после удаления таблицы
func deleteTableInList(tableName string) (*TablesList, error) {
	// Пытаемся прочитать существующий список таблиц
	tablesList, err := readTableListFile()
	if err != nil {
		return nil, fmt.Errorf("failed to read tables list: %w", err)
	}

	// Удаляем таблицу из списка
	delete(tablesList.Tables, tableName)

	// Записываем обновленный список
	tablesList, err = writeTablesListFile(tablesList)
	if err != nil {
		return nil, fmt.Errorf("failed to write tables list: %w", err)
	}

	return tablesList, nil
}
