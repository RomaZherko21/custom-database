package disk_manager

import (
	"encoding/binary"
	"fmt"
	"os"
)

// Магическое число для page directory файла
const PAGE_DIRECTORY_MAGIC_NUMBER = 0x8ABCDEF1

// Размер заголовка page directory файла
const PAGE_DIRECTORY_HEADER_SIZE = 12

type PageDirectoryHeader struct {
	MagicNumber uint32 // 4 байта - идентификатор page directory файла (0x8ABCDEF1)
	PageCount   uint32 // 4 байта - количество страниц в директории
	NextPageID  uint32 // 4 байта - следующий PageID для новых страниц
}

func NewPageDirectoryHeader(pageCount uint32) *PageDirectoryHeader {
	return &PageDirectoryHeader{
		MagicNumber: PAGE_DIRECTORY_MAGIC_NUMBER,
		PageCount:   pageCount,
		NextPageID:  0,
	}
}

// Serialize сериализует PageDirectoryHeader в байты
func (header *PageDirectoryHeader) Serialize() []byte {
	data := make([]byte, PAGE_DIRECTORY_HEADER_SIZE)

	// Записываем MagicNumber (байты 0-4)
	binary.BigEndian.PutUint32(data[0:4], header.MagicNumber)

	// Записываем PageCount (байты 4-8)
	binary.BigEndian.PutUint32(data[4:8], header.PageCount)

	// Записываем NextPageID (байты 8-12)
	binary.BigEndian.PutUint32(data[8:12], header.NextPageID)

	return data
}

func (header *PageDirectoryHeader) Deserialize(data []byte) (*PageDirectoryHeader, error) {
	if len(data) < PAGE_DIRECTORY_HEADER_SIZE {
		return nil, fmt.Errorf("insufficient data for page directory header")
	}

	return &PageDirectoryHeader{
		MagicNumber: binary.BigEndian.Uint32(data[0:4]),
		PageCount:   binary.BigEndian.Uint32(data[4:8]),
		NextPageID:  binary.BigEndian.Uint32(data[8:12]),
	}, nil
}

const PAGE_DIRECTORY_ENTRY_SIZE = 12

type PageDirectoryEntry struct {
	PageID    uint32 // 4 байта - ID страницы
	FreeSpace uint32 // 4 байта - свободное место в странице
	Flags     uint32 // 4 байта - флаги страницы
}

func NewPageDirectoryEntry(pageID uint32, freeSpace uint32, flags uint32) *PageDirectoryEntry {
	return &PageDirectoryEntry{
		PageID:    pageID,
		FreeSpace: freeSpace,
		Flags:     flags,
	}
}

// Serialize сериализует PageDirectoryEntry в байты
func (entry *PageDirectoryEntry) Serialize() []byte {
	data := make([]byte, PAGE_DIRECTORY_ENTRY_SIZE)

	// Записываем PageID (байты 0-4)
	binary.BigEndian.PutUint32(data[0:4], entry.PageID)

	// Записываем FreeSpace (байты 4-8)
	binary.BigEndian.PutUint32(data[4:8], entry.FreeSpace)

	// Записываем Flags (байты 8-12)
	binary.BigEndian.PutUint32(data[8:12], entry.Flags)

	return data
}

func (entry *PageDirectoryEntry) Deserialize(data []byte) (*PageDirectoryEntry, error) {
	if len(data) < PAGE_DIRECTORY_ENTRY_SIZE {
		return nil, fmt.Errorf("insufficient data for page directory entry")
	}

	return &PageDirectoryEntry{
		PageID:    binary.BigEndian.Uint32(data[0:4]),
		FreeSpace: binary.BigEndian.Uint32(data[4:8]),
		Flags:     binary.BigEndian.Uint32(data[8:12]),
	}, nil
}

const PAGE_DIRECTORY_FILE_PATH = "tables/%s.dir"

type PageDirectory struct {
	TableName string
	Header    *PageDirectoryHeader
	Entries   []PageDirectoryEntry
}

func CreatePageDirectory(tableName string, entries []PageDirectoryEntry) (*PageDirectory, error) {
	// Проверяем, существует ли page directory файл
	if _, err := os.Stat(fmt.Sprintf(PAGE_DIRECTORY_FILE_PATH, tableName)); err == nil {
		return nil, fmt.Errorf("page directory for table %s already exists", tableName)
	}

	// Создаем .dir файл в папке tables
	dirFilePath := fmt.Sprintf(PAGE_DIRECTORY_FILE_PATH, tableName)
	dirFile, err := os.Create(dirFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create page directory file: %w", err)
	}
	defer dirFile.Close()

	// Записываем заголовок в page directory файл
	header := NewPageDirectoryHeader(uint32(len(entries)))
	_, err = dirFile.Write(header.Serialize())
	if err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	// Записываем записи в page directory файл
	for _, entry := range entries {
		_, err = dirFile.Write(entry.Serialize())
		if err != nil {
			return nil, fmt.Errorf("failed to write entry: %w", err)
		}
	}

	return &PageDirectory{
		TableName: tableName,
		Header:    header,
		Entries:   entries,
	}, nil
}

func ReadPageDirectory(tableName string) (*PageDirectory, error) {
	// Проверяем, существует ли page directory файл
	if _, err := os.Stat(fmt.Sprintf(PAGE_DIRECTORY_FILE_PATH, tableName)); err != nil {
		return nil, fmt.Errorf("page directory for table %s not found", tableName)
	}

	// Читаем page directory файл
	dirFile, err := os.Open(fmt.Sprintf(PAGE_DIRECTORY_FILE_PATH, tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to open page directory file: %w", err)
	}
	defer dirFile.Close()

	// Читаем заголовок
	headerBytes := make([]byte, PAGE_DIRECTORY_HEADER_SIZE)
	n, err := dirFile.Read(headerBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}
	if n != PAGE_DIRECTORY_HEADER_SIZE {
		return nil, fmt.Errorf("incomplete header read: got %d bytes, expected %d", n, PAGE_DIRECTORY_HEADER_SIZE)
	}

	// Десериализуем заголовок
	header, err := (&PageDirectoryHeader{}).Deserialize(headerBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize header: %w", err)
	}

	// Читаем записи
	entries := make([]PageDirectoryEntry, header.PageCount)
	for i := 0; i < int(header.PageCount); i++ {
		entryBytes := make([]byte, PAGE_DIRECTORY_ENTRY_SIZE)
		n, err := dirFile.Read(entryBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to read entry %d: %w", i, err)
		}
		if n != PAGE_DIRECTORY_ENTRY_SIZE {
			return nil, fmt.Errorf("incomplete entry %d read: got %d bytes, expected %d", i, n, PAGE_DIRECTORY_ENTRY_SIZE)
		}

		// Десериализуем запись
		entry, err := (&PageDirectoryEntry{}).Deserialize(entryBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize entry %d: %w", i, err)
		}
		entries[i] = *entry
	}

	return &PageDirectory{
		TableName: tableName,
		Header:    header,
		Entries:   entries,
	}, nil
}

func DeletePageDirectory(tableName string) error {
	// Проверяем, существует ли page directory файл
	if _, err := os.Stat(fmt.Sprintf(PAGE_DIRECTORY_FILE_PATH, tableName)); err != nil {
		return fmt.Errorf("page directory for table %s not found", tableName)
	}

	// Удаляем .dir файл
	dirFilePath := fmt.Sprintf(PAGE_DIRECTORY_FILE_PATH, tableName)
	return os.Remove(dirFilePath)
}

// AddPage добавляет новую страницу в page directory
func (pd *PageDirectory) AddPage(freeSpace uint32, flags uint32) error {
	newPageID := pd.Header.NextPageID
	entry := NewPageDirectoryEntry(newPageID, freeSpace, flags)

	// Записываем в page directory файл
	dirFile, err := os.Open(fmt.Sprintf(PAGE_DIRECTORY_FILE_PATH, pd.TableName))
	if err != nil {
		return fmt.Errorf("failed to open page directory file: %w", err)
	}
	defer dirFile.Close()

	// Записываем page в page directory файл
	_, err = dirFile.Write(entry.Serialize())
	if err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}

	pd.Entries = append(pd.Entries, *entry)
	pd.Header.PageCount++
	pd.Header.NextPageID++

	return nil
}

// UpdatePage обновляет информацию о странице в page directory
func (pd *PageDirectory) UpdatePage(pageID uint32, freeSpace uint32, flags uint32) error {
	// Проверяем, существует ли page directory файл
	if _, err := os.Stat(fmt.Sprintf(PAGE_DIRECTORY_FILE_PATH, pd.TableName)); err != nil {
		return fmt.Errorf("page directory for table %s not found", pd.TableName)
	}

	// Читаем page directory файл
	dirFile, err := os.Open(fmt.Sprintf(PAGE_DIRECTORY_FILE_PATH, pd.TableName))
	if err != nil {
		return fmt.Errorf("failed to open page directory file: %w", err)
	}
	defer dirFile.Close()

	// Обновляем конкретный page в page directory файлe
	entry := NewPageDirectoryEntry(pageID, freeSpace, flags)
	_, err = dirFile.Write(entry.Serialize())
	if err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}
	// Рассчитываем offset для перезаписи
	offset := int64(PAGE_DIRECTORY_HEADER_SIZE) + (int64(pageID) * int64(PAGE_DIRECTORY_ENTRY_SIZE))
	dirFile.WriteAt(entry.Serialize(), offset)

	// Обновляем запись в page directory файл
	for i, entry := range pd.Entries {
		if entry.PageID == pageID {
			pd.Entries[i].FreeSpace = freeSpace
			pd.Entries[i].Flags = flags
			return nil
		}
	}
	return fmt.Errorf("page %d not found in directory", pageID)
}

// FindPageWithSpace находит страницу с достаточным свободным местом
func (pd *PageDirectory) FindPageWithSpace(requiredSpace uint32) *PageDirectoryEntry {
	for i := range pd.Entries {
		if pd.Entries[i].FreeSpace >= requiredSpace {
			return &pd.Entries[i]
		}
	}
	return nil
}

// TODO: добавить функцию для сохранения изменений в файл
// TODO: добавить функцию для удаления страниц
