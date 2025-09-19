package disk_manager

import (
	"encoding/binary"
	"fmt"
)

type DataType uint32

const (
	INT_32_TYPE DataType = 1
	TEXT_TYPE   DataType = 2
)

// PageID представляет идентификатор страницы
type PageID struct {
	FileID     uint32 // ID файла таблицы
	PageNumber uint32 // Номер страницы в файле
}

// RowID представляет идентификатор строки
type RowID struct {
	FileID     uint32 // ID файла таблицы
	PageID     uint32 // Номер страницы в файле
	SlotNumber uint16 // Номер слота в странице
}

// PageHeader содержит метаданные страницы
type PageHeader struct {
	LSN      uint64 // Log Sequence Number для WAL
	Checksum uint16 // Контрольная сумма страницы
	Flags    uint16 // Флаги страницы
	Lower    uint16 // Начало свободного места (конец Item Pointers)
	Upper    uint16 // Конец свободного места (начало данных)
	Special  uint16 // Начало специальной области
	PageSize uint16 // Размер страницы + версия
	PruneXID uint32 // XID для очистки
}

// PageSlot указывает на запись в странице
type PageSlot struct {
	Offset uint16 // Смещение записи в странице
	Length uint16 // Длина записи
}

// Record представляет строку данных
type Record struct {
	RowID RowID  // Идентификатор строки
	Data  []byte // Данные строки
}

// DataPage представляет страницу данных
type DataPage struct {
	Header       PageHeader // Заголовок страницы
	ItemPointers []PageSlot // Массив указателей на записи
	Data         []byte     // Данные страницы
	FreeSpace    int        // Количество свободных байт
}

// Serialize преобразует PageHeader в байты
func (ph *PageHeader) Serialize() []byte {
	data := make([]byte, PAGE_HEADER_SIZE)

	binary.BigEndian.PutUint64(data[0:8], ph.LSN)
	binary.BigEndian.PutUint16(data[8:10], ph.Checksum)
	binary.BigEndian.PutUint16(data[10:12], ph.Flags)
	binary.BigEndian.PutUint16(data[12:14], ph.Lower)
	binary.BigEndian.PutUint16(data[14:16], ph.Upper)
	binary.BigEndian.PutUint16(data[16:18], ph.Special)
	binary.BigEndian.PutUint16(data[18:20], ph.PageSize)
	binary.BigEndian.PutUint32(data[20:24], ph.PruneXID)

	return data
}

// Deserialize создает PageHeader из байтов
func DeserializePageHeader(data []byte) (*PageHeader, error) {
	if len(data) < PAGE_HEADER_SIZE {
		return nil, fmt.Errorf("insufficient data for page header")
	}

	return &PageHeader{
		LSN:      binary.BigEndian.Uint64(data[0:8]),
		Checksum: binary.BigEndian.Uint16(data[8:10]),
		Flags:    binary.BigEndian.Uint16(data[10:12]),
		Lower:    binary.BigEndian.Uint16(data[12:14]),
		Upper:    binary.BigEndian.Uint16(data[14:16]),
		Special:  binary.BigEndian.Uint16(data[16:18]),
		PageSize: binary.BigEndian.Uint16(data[18:20]),
		PruneXID: binary.BigEndian.Uint32(data[20:24]),
	}, nil
}
