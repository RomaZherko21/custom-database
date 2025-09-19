package disk_manager

import (
	"errors"
)

// DiskManager интерфейс для работы с диском
// Используется Buffer Pool для кэширования страниц
type DiskManager interface {
	// Управление таблицами
	CreateTable(tableName string, schema *TableSchema) error
	// DropTable(tableName string) error
	// TableExists(tableName string) bool
	// GetTableSchema(tableName string) (*TableSchema, error)

	// // Работа со страницами (основной интерфейс для Buffer Pool)
	// ReadPage(pageID PageID) (*DataPage, error)
	// WritePage(pageID PageID, page *DataPage) error
	// AllocatePage(tableName string) (PageID, error)
	// FreePage(pageID PageID) error

	// // Работа с записями (высокоуровневый интерфейс)
	// InsertRecord(tableName string, data []byte) (RowID, error)
	// GetRecord(rowID RowID) (*Record, error)
	// UpdateRecord(rowID RowID, newData []byte) error
	// DeleteRecord(rowID RowID) error

	// // Управление свободным местом
	// FindFreeSlot(tableName string, dataSize int) (PageID, uint16, error)
	// GetPageFreeSpace(pageID PageID) (int, error)
	// CompactPage(pageID PageID) error

	// // Статистика и информация
	// GetPageInfo(pageID PageID) (*PageInfo, error)
	// GetTableStats(tableName string) (*TableStats, error)
	// GetDatabaseStats() (*DatabaseStats, error)

	// // Управление метаданными
	// LoadMetadata() error
	// SaveMetadata() error
	// Sync() error // Синхронизация с диском
}

type diskManager struct {
}

func NewDiskManager() DiskManager {
	return &diskManager{}
}

// PageInfo содержит информацию о странице
type PageInfo struct {
	PageID       PageID     `json:"page_id"`
	PageType     uint8      `json:"page_type"`     // 0=data, 1=index, 2=free
	RecordCount  uint16     `json:"record_count"`  // Количество записей
	FreeSpace    uint16     `json:"free_space"`    // Свободное место в байтах
	TotalSlots   uint16     `json:"total_slots"`   // Общее количество слотов
	UsedSlots    uint16     `json:"used_slots"`    // Используемые слоты
	IsDirty      bool       `json:"is_dirty"`      // Изменена ли страница
	LastAccessed uint64     `json:"last_accessed"` // Время последнего доступа
	Header       PageHeader `json:"header"`        // Заголовок страницы
}

// TableStats содержит статистику таблицы
type TableStats struct {
	TableName      string `json:"table_name"`
	FileID         uint32 `json:"file_id"`
	TotalPages     uint32 `json:"total_pages"`
	TotalRecords   uint32 `json:"total_records"`
	TotalFreeSpace uint32 `json:"total_free_space"`
	FileSize       int64  `json:"file_size"`
	AvgRecordSize  uint16 `json:"avg_record_size"`
	LastUpdated    uint64 `json:"last_updated"`
}

// DatabaseStats содержит статистику базы данных
type DatabaseStats struct {
	TotalTables     uint32 `json:"total_tables"`
	TotalPages      uint32 `json:"total_pages"`
	TotalRecords    uint32 `json:"total_records"`
	TotalFileSize   int64  `json:"total_file_size"`
	FreePages       uint32 `json:"free_pages"`
	UsedPages       uint32 `json:"used_pages"`
	DatabaseVersion uint16 `json:"database_version"`
}

// Ошибки DiskManager
var (
	ErrTableNotFound     = errors.New("table not found")
	ErrPageNotFound      = errors.New("page not found")
	ErrRecordNotFound    = errors.New("record not found")
	ErrInsufficientSpace = errors.New("insufficient space")
	ErrInvalidPageID     = errors.New("invalid page ID")
	ErrInvalidRowID      = errors.New("invalid row ID")
	ErrTableExists       = errors.New("table already exists")
	ErrCorruptedData     = errors.New("corrupted data")
	ErrIOError           = errors.New("I/O error")
)

// Константы типов страниц
const (
	PageTypeData  = 0 // Страница с данными
	PageTypeIndex = 1 // Страница индекса
	PageTypeFree  = 2 // Свободная страница
)

// Константы типов данных
const (
	DataTypeInt   = 1 // Целое число
	DataTypeText  = 2 // Текст
	DataTypeFloat = 3 // Число с плавающей точкой
	DataTypeBool  = 4 // Логическое значение
)

// Константы флагов колонок
const (
	ColumnFlagNullable   = 0x01 // Может быть NULL
	ColumnFlagPrimaryKey = 0x02 // Первичный ключ
	ColumnFlagUnique     = 0x04 // Уникальное значение
	ColumnFlagIndexed    = 0x08 // Индексированная колонка
)
