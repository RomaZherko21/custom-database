package disk_manager

import (
	"encoding/binary"
	"fmt"
	"os"
)

type DataFileHeader struct {
	MagicNumber uint32 // 4 байта - идентификатор формата файла
	PagesCount  uint32 // 4 байта - общее количество страниц
	RecordCount uint32 // 4 байта - общее количество записей
}

const DATA_FILE_HEADER_SIZE = 12

func (header *DataFileHeader) Serialize() []byte {
	data := make([]byte, DATA_FILE_HEADER_SIZE)

	// Записываем MagicNumber (байты 0-4)
	binary.BigEndian.PutUint32(data[0:4], header.MagicNumber)

	// Записываем PagesCount (байты 4-8)
	binary.BigEndian.PutUint32(data[4:8], header.PagesCount)

	// Записываем RecordCount (байты 8-12)
	binary.BigEndian.PutUint32(data[8:12], header.RecordCount)

	return data
}

func (header *DataFileHeader) Deserialize(data []byte) (*DataFileHeader, error) {
	if len(data) < DATA_FILE_HEADER_SIZE {
		return nil, fmt.Errorf("insufficient data for data file header")
	}

	return &DataFileHeader{
		MagicNumber: binary.BigEndian.Uint32(data[0:4]),
		PagesCount:  binary.BigEndian.Uint32(data[4:8]),
		RecordCount: binary.BigEndian.Uint32(data[8:12]),
	}, nil
}

type DataFile struct {
	Header *DataFileHeader
	Pages  []*RawPage
}

// Создает data файл, помним что пустой page мы не создаем,
// он будет создан через AddPage в buffer pool
func createDataFile(tableName string) (*DataFile, error) {
	dataFilePath := fmt.Sprintf(DATA_FILE_PATH, tableName)

	// Проверяем, существует ли data файл
	if _, err := os.Stat(dataFilePath); err == nil {
		return nil, fmt.Errorf("data file for table %s already exists", tableName)
	}

	// Создаем data файл
	dataFile, err := os.Create(dataFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create data file: %w", err)
	}
	defer dataFile.Close()

	// Записываем заголовок в data файл
	header := &DataFileHeader{
		MagicNumber: DATA_FILE_MAGIC_NUMBER,
		PagesCount:  0,
		RecordCount: 0,
	}
	_, err = dataFile.Write(header.Serialize())
	if err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	return &DataFile{
		Header: header,
		Pages:  make([]*RawPage, 0),
	}, nil
}

// readDataFileHeader читает только заголовок файла
func readDataFileHeader(tableName string) (*DataFile, error) {
	dataFilePath := fmt.Sprintf(DATA_FILE_PATH, tableName)

	// Проверяем, что файл существует
	if _, err := os.Stat(dataFilePath); err != nil {
		return nil, fmt.Errorf("data file for table %s not found", tableName)
	}

	dataFile, err := os.Open(dataFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}
	defer dataFile.Close()

	headerBytes := make([]byte, DATA_FILE_HEADER_SIZE)

	// Читаем файл
	n, err := dataFile.ReadAt(headerBytes, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read data file: %w", err)
	}
	if n != DATA_FILE_HEADER_SIZE {
		return nil, fmt.Errorf("incomplete header read: got %d bytes, expected %d", n, DATA_FILE_HEADER_SIZE)
	}
	// проверяем на magic number
	if binary.BigEndian.Uint32(headerBytes[0:4]) != DATA_FILE_MAGIC_NUMBER {
		return nil, fmt.Errorf("invalid magic number")
	}

	// Проверяем минимальный размер файла
	if len(headerBytes) < DATA_FILE_HEADER_SIZE {
		return nil, fmt.Errorf("data file too small")
	}

	// Десериализуем заголовок
	header, err := (&DataFileHeader{}).Deserialize(headerBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize header: %w", err)
	}

	return &DataFile{
		Header: header,
		Pages:  make([]*RawPage, 0),
	}, nil
}

func deleteDataFile(tableName string) error {
	dataFilePath := fmt.Sprintf(DATA_FILE_PATH, tableName)

	// Проверяем, что файл существует
	if _, err := os.Stat(dataFilePath); err != nil {
		return fmt.Errorf("data file for table %s not found", tableName)
	}

	os.Remove(dataFilePath)
	return nil
}

// addPage добавляет новую страницу в data файл
// Лучше использовать когда место на предыдущей странице закончилось
func (df *DataFile) addPage(tableName string) error {
	dataFilePath := fmt.Sprintf(DATA_FILE_PATH, tableName)

	// Проверяем, что файл существует
	if _, err := os.Stat(dataFilePath); err != nil {
		return fmt.Errorf("data file for table %s not found", tableName)
	}

	// Открываем файл для записи
	dataFile, err := os.OpenFile(dataFilePath, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open data file: %w", err)
	}
	defer dataFile.Close()

	// Обновляем счетчик страниц в заголовке
	df.Header.PagesCount++

	// Создаем новую страницу
	page := newPage(PageID{PageNumber: df.Header.PagesCount})

	// Добавляем страницу в конец файла
	pageOffset := DATA_FILE_HEADER_SIZE + (df.Header.PagesCount-1)*PAGE_SIZE
	_, err = dataFile.WriteAt(page.Serialize(), int64(pageOffset))
	if err != nil {
		return fmt.Errorf("failed to write page: %w", err)
	}

	// Обновляем заголовок файла
	_, err = dataFile.WriteAt(df.Header.Serialize(), 0)
	if err != nil {
		return fmt.Errorf("failed to update header: %w", err)
	}

	return nil
}

func (df *DataFile) readPage(tableName string, pageID PageID) (*RawPage, error) {
	dataFilePath := fmt.Sprintf(DATA_FILE_PATH, tableName)

	if pageID.PageNumber > df.Header.PagesCount {
		return nil, fmt.Errorf("page id %d is out of range", pageID)
	}

	// Проверяем, что файл существует
	if _, err := os.Stat(dataFilePath); err != nil {
		return nil, fmt.Errorf("data file for table %s not found", tableName)
	}

	// Читаем файл
	data, err := os.ReadFile(dataFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read page: %w", err)
	}

	offset := DATA_FILE_HEADER_SIZE + (pageID.PageNumber-1)*PAGE_SIZE
	page, err := (&RawPage{}).Deserialize(data[offset : offset+PAGE_SIZE])
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize page: %w", err)
	}

	return page, nil
}

func (df *DataFile) writePage(tableName string, pageID PageID, page *RawPage) error {
	dataFilePath := fmt.Sprintf(DATA_FILE_PATH, tableName)

	// Открываем файл для записи
	dataFile, err := os.OpenFile(dataFilePath, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open data file: %w", err)
	}
	defer dataFile.Close()

	offset := DATA_FILE_HEADER_SIZE + (pageID.PageNumber-1)*PAGE_SIZE
	_, err = dataFile.WriteAt(page.Serialize(), int64(offset))
	if err != nil {
		return fmt.Errorf("failed to write page: %w", err)
	}

	return nil
}
