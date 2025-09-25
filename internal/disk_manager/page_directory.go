package disk_manager

import (
	"encoding/binary"
	"fmt"
	"os"
)

// Размер заголовка page directory файла
const PAGE_DIRECTORY_HEADER_SIZE = 12

type PageDirectoryHeader struct {
	MagicNumber uint32 // 4 байта - идентификатор page directory файла
	PageCount   uint32 // 4 байта - количество страниц в директории
	NextPageID  uint32 // 4 байта - следующий PageID для новых страниц
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
	Flags     uint32 // 4 байта - флаги страницы, 0 - активная, 1 - удаленная
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

type PageDirectory struct {
	TableName string
	Header    *PageDirectoryHeader
	Entries   []PageDirectoryEntry
}

// Создает page directory файл, помним что пустой page мы не создаем,
// он будет создан через AddNewPage в buffer pool
func createPageDirectoryFile(tableName string) (*PageDirectory, error) {
	dirFilePath := fmt.Sprintf(PAGE_DIRECTORY_FILE_PATH, tableName)

	// Проверяем, существует ли page directory файл
	if _, err := os.Stat(dirFilePath); err == nil {
		return nil, fmt.Errorf("page directory for table %s already exists", tableName)
	}

	// Создаем .dir файл в папке tables
	dirFile, err := os.Create(dirFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create page directory file: %w", err)
	}
	defer dirFile.Close()

	// Записываем заголовок в page directory файл, с одним пустым page
	header := &PageDirectoryHeader{
		MagicNumber: PAGE_DIRECTORY_MAGIC_NUMBER,
		PageCount:   0, // У нас пока нет страниц
		NextPageID:  PAGE_INITIAL_ID,
	}
	_, err = dirFile.Write(header.Serialize())
	if err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	return &PageDirectory{
		TableName: tableName,
		Header:    header,
		Entries:   []PageDirectoryEntry{},
	}, nil
}

func readPageDirectory(tableName string) (*PageDirectory, error) {
	dirFilePath := fmt.Sprintf(PAGE_DIRECTORY_FILE_PATH, tableName)

	// Проверяем, существует ли page directory файл
	if _, err := os.Stat(dirFilePath); err != nil {
		return nil, fmt.Errorf("page directory for table %s not found", tableName)
	}

	// Читаем page directory файл
	dirFile, err := os.Open(dirFilePath)
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
	// проверяем на magic number
	if binary.BigEndian.Uint32(headerBytes[0:4]) != PAGE_DIRECTORY_MAGIC_NUMBER {
		return nil, fmt.Errorf("invalid magic number")
	}

	// Десериализуем заголовок
	header, err := (&PageDirectoryHeader{}).Deserialize(headerBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize header: %w", err)
	}

	// Читаем pages
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

func writePageDirectory(tableName string, pageDirectory *PageDirectory) (*PageDirectory, error) {
	dirFilePath := fmt.Sprintf(PAGE_DIRECTORY_FILE_PATH, tableName)

	// Проверяем, существует ли page directory файл
	if _, err := os.Stat(dirFilePath); err != nil {
		return nil, fmt.Errorf("page directory for table %s not found", tableName)
	}

	// Создаем пустой массив байт
	data := make([]byte, 0)

	// Записываем заголовок
	data = append(data, pageDirectory.Header.Serialize()...)

	// Записываем page directory entries
	for _, entry := range pageDirectory.Entries {
		data = append(data, entry.Serialize()...)
	}

	// Открываем файл для записи
	dirFile, err := os.OpenFile(dirFilePath, os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open page directory file: %w", err)
	}
	defer dirFile.Close()

	_, err = dirFile.Write(data)
	if err != nil {
		return nil, fmt.Errorf("failed to write data: %w", err)
	}

	return pageDirectory, nil
}

func deletePageDirectory(tableName string) error {
	dirFilePath := fmt.Sprintf(PAGE_DIRECTORY_FILE_PATH, tableName)

	// Проверяем, существует ли page directory файл
	if _, err := os.Stat(dirFilePath); err != nil {
		return fmt.Errorf("page directory for table %s not found", tableName)
	}

	// Удаляем .dir файл
	return os.Remove(dirFilePath)
}
