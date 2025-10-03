package buffer_bool

import (
	"custom-database/internal/disk_manager"
	"errors"
	"fmt"
	"time"
)

// BufferPoolInterface интерфейс для Buffer Pool
type BufferPoolInterface interface {
	// GetPage - метод для получения страницы из буфера
	// это нужно для того чтобы мы могли получить страницу из буфера
	// и использовать ее в нашем коде, менять данные по поинтеру
	GetPage(tableName string, pageID disk_manager.PageID) (*BufferFrame, error)

	// MarkDirty - метод для отметки страницы как измененной
	// это нужно для того чтобы мы могли записать измененную страницу на диск
	MarkDirty(tableName string, pageID disk_manager.PageID)

	// Unpin - метод для освобождения страницы
	// это нужно для того чтобы мы могли освободить память, если мы больше не используем страницу
	// Pin страницы будет происходить в GetPage
	Unpin(tableName string, pageID disk_manager.PageID)

	// AddNewPage - создает новую страницу в таблице
	AddNewPage(tableName string, pageID disk_manager.PageID) (*BufferFrame, error)

	// Управление таблицами
	CreateTable(tableName string, columns []disk_manager.ColumnInfo) error
	DropTable(tableName string) error

	// Работа с метаинформацией
	ReadMetaInfo(tableName string) (*MetaInfo, error)
	// WriteMetaInfo записывает метаинформацию таблицы на диск
	// Перед вызовом, все необходимые данные в *MetaInfo нужно изменить
	WriteMetaInfo(tableName string) error
}

// BufferPool реализация Buffer Pool с LRU-K и Disk Scheduler
type BufferPool struct {
	// Основные компоненты
	Pages     map[disk_manager.PageID]*BufferFrame // Кэш страниц
	MaxSize   int                                  // Максимальный размер пула
	MetaInfo  map[string]*MetaInfo                 // Кэш метаинформации таблиц
	TableList *disk_manager.TablesList             // Кэш списка всех таблиц

	// Компоненты для работы с диском
	DiskManager disk_manager.DiskManager // Диск менеджер

	// Компоненты для работы с LRU-K вытеснением страниц
	LRUKCache *LRUKCache // LRU-K кэш для замещения

	// Компоненты для работы background worker
	DiskScheduler *diskScheduler // Планировщик дисковых операций

	// Управление памятью
	DirtyPages map[disk_manager.PageID]bool // Отслеживание измененных страниц
	PinCounts  map[disk_manager.PageID]int  // Счетчики закреплений страниц
}

type MetaInfo struct {
	MetaData      *disk_manager.MetaData
	PageDirectory *disk_manager.PageDirectory
	DataHeaders   *disk_manager.DataFileHeader
}

// BufferFrame представляет фрейм в Buffer Pool
type BufferFrame struct {
	PageID       disk_manager.PageID // ID страницы
	TableName    string              // Имя таблицы
	Page         *disk_manager.Page  // Данные страницы
	IsDirty      bool                // Флаг изменений
	IsPinned     bool                // Флаг закрепления
	LastAccessed time.Time           // Время последнего доступа
	PinCount     int                 // Счетчик закреплений
}

// NewBufferPool создает новый Buffer Pool
func NewBufferPool(maxSize int, k int) (BufferPoolInterface, error) {
	// Создаем LRU-K кэш
	lruKCache := NewLRUKCache(k, maxSize)

	// Создаем Disk Manager
	diskManager := disk_manager.NewDiskManager()

	// TODO: решить когда вызывать CreateDataBase()
	err := diskManager.CreateDataBase()
	if err != nil {
		return nil, err
	}

	// Создаем Disk Scheduler
	diskScheduler := NewDiskScheduler()

	// Инициализируем список таблиц
	tableList := readTableList(diskManager)
	// Инициализируем метаинформацию всех таблиц из списка
	metaInfo := make(map[string]*MetaInfo)
	for tableName := range tableList.Tables {
		metaInfo[tableName], err = readMetaInfo(diskManager, tableName)
		if err != nil {
			return nil, err
		}
	}

	bp := &BufferPool{
		Pages:         make(map[disk_manager.PageID]*BufferFrame),
		MetaInfo:      metaInfo,
		TableList:     tableList,
		LRUKCache:     lruKCache,
		DiskScheduler: diskScheduler,
		DiskManager:   diskManager,
		MaxSize:       maxSize,
		DirtyPages:    make(map[disk_manager.PageID]bool),
		PinCounts:     make(map[disk_manager.PageID]int),
	}

	err = bp.startBgWorker()
	if err != nil {
		return nil, err
	}

	return bp, nil
}

// GetPage получает страницу из буфера
func (bp *BufferPool) GetPage(tableName string, pageID disk_manager.PageID) (*BufferFrame, error) {
	// Проверяем кэш
	if frame, exists := bp.Pages[pageID]; exists {
		bp.LRUKCache.Access(pageID)
		frame.LastAccessed = time.Now()
		frame.PinCount++
		frame.IsPinned = true
		bp.PinCounts[pageID]++
		return frame, nil
	}

	// Если буфер полон, нужно вытеснить страницу
	if len(bp.Pages) >= bp.MaxSize {
		err := bp.evictPage()
		if err != nil {
			return nil, err
		}
	}

	// Читаем страницу с диска синхронно
	page, err := bp.DiskManager.ReadPage(tableName, pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to read page from disk: %w", err)
	}

	// Создаем новый фрейм
	frame := &BufferFrame{
		PageID:       pageID,
		TableName:    tableName,
		Page:         page,
		IsDirty:      false,
		IsPinned:     true,
		LastAccessed: time.Now(),
		PinCount:     1,
	}

	// Добавляем в кэш
	bp.Pages[pageID] = frame
	bp.PinCounts[pageID] = 1
	bp.LRUKCache.Access(pageID)

	return frame, nil
}

// MarkDirty отмечает страницу как измененную
func (bp *BufferPool) MarkDirty(tableName string, pageID disk_manager.PageID) {
	if frame, exists := bp.Pages[pageID]; exists {
		frame.IsDirty = true
		bp.DirtyPages[pageID] = true
	}
}

// Unpin освобождает страницу из памяти
func (bp *BufferPool) Unpin(tableName string, pageID disk_manager.PageID) {
	if frame, exists := bp.Pages[pageID]; exists {
		frame.PinCount--
		bp.PinCounts[pageID]--

		if frame.PinCount <= 0 {
			frame.IsPinned = false
			bp.PinCounts[pageID] = 0
		}
	}
}

// AddNewPage создает новую страницу в таблице
func (bp *BufferPool) AddNewPage(tableName string, pageID disk_manager.PageID) (*BufferFrame, error) {
	page, err := bp.DiskManager.AddNewPage(tableName, pageID)
	if err != nil {
		return nil, err
	}

	// Если буфер полон, вытесняем страницу
	if len(bp.Pages) >= bp.MaxSize {
		err := bp.evictPage()
		if err != nil {
			return nil, err
		}
	}

	// Создаем фрейм
	frame := &BufferFrame{
		PageID:       pageID,
		TableName:    tableName,
		Page:         page,
		IsDirty:      false, // Новая страница помечается как dirty
		IsPinned:     true,
		LastAccessed: time.Now(),
		PinCount:     1,
	}

	// Добавляем в кэш
	bp.Pages[pageID] = frame
	bp.PinCounts[pageID] = 1
	bp.DirtyPages[pageID] = false
	bp.LRUKCache.Access(pageID)

	return frame, nil
}

// startBgWorker запускает background worker
func (bp *BufferPool) startBgWorker() error {
	// Запускаем background worker в disk scheduler
	return bp.DiskScheduler.StartBgWorker(bp.flushDirtyPages)
}

// CreateTable создает новую таблицу
func (bp *BufferPool) CreateTable(tableName string, columns []disk_manager.ColumnInfo) error {
	// Создаем таблицу через DiskManager
	err := bp.DiskManager.CreateTable(tableName, columns)
	if err != nil {
		return err
	}

	// Читаем метаинформацию новой таблицы
	metaInfo, err := readMetaInfo(bp.DiskManager, tableName)
	if err != nil {
		return err
	}

	// Кэшируем метаинформацию
	bp.MetaInfo[tableName] = metaInfo

	// Обновляем список таблиц
	bp.TableList = readTableList(bp.DiskManager)

	return nil
}

// DropTable удаляет таблицу
func (bp *BufferPool) DropTable(tableName string) error {
	// Удаляем таблицу через DiskManager
	err := bp.DiskManager.DropTable(tableName)
	if err != nil {
		return err
	}

	// Удаляем метаинформацию из кэша
	delete(bp.MetaInfo, tableName)

	// Обновляем список таблиц
	bp.TableList = readTableList(bp.DiskManager)

	return nil
}

func (bp *BufferPool) ReadMetaInfo(tableName string) (*MetaInfo, error) {
	return bp.MetaInfo[tableName], nil
}

func (bp *BufferPool) WriteMetaInfo(tableName string) error {
	metaInfo, exists := bp.MetaInfo[tableName]
	if !exists || metaInfo == nil {
		return fmt.Errorf("meta info for table %s not found", tableName)
	}
	return writeMetaInfo(bp.DiskManager, tableName, metaInfo)
}

// evictPage вытесняет страницу из буфера
func (bp *BufferPool) evictPage() error {
	// Создаем функцию проверки pin-статуса
	checkPinStatus := func(pageID disk_manager.PageID) bool {
		return bp.PinCounts[pageID] > 0
	}

	// Получаем кандидата на вытеснение от LRU-K с проверкой pin-статуса
	victimPageID := bp.LRUKCache.GetVictim(checkPinStatus)
	if victimPageID.PageNumber == 0 {
		return errors.New("no evictable pages found (all pages are pinned)")
	}

	// Получаем фрейм
	frame, exists := bp.Pages[victimPageID]
	if !exists {
		return errors.New("victim page not found in buffer pool")
	}

	// Если страница dirty, записываем на диск
	if frame.IsDirty {
		_, err := bp.DiskManager.WritePage(frame.TableName, victimPageID, frame.Page)
		if err != nil {
			return fmt.Errorf("failed to write dirty page: %w", err)
		}
	}

	// Удаляем из буфера
	delete(bp.Pages, victimPageID)
	delete(bp.DirtyPages, victimPageID)
	delete(bp.PinCounts, victimPageID)
	bp.LRUKCache.Evict(victimPageID)

	return nil
}

// flushDirtyPages записывает все dirty страницы на диск
func (bp *BufferPool) flushDirtyPages() {
	dirtyPages := make([]disk_manager.PageID, 0, len(bp.DirtyPages))
	for pageID := range bp.DirtyPages {
		dirtyPages = append(dirtyPages, pageID)
	}

	// Записываем dirty страницы
	for _, pageID := range dirtyPages {
		frame, exists := bp.Pages[pageID]
		if !exists {
			continue
		}

		// Записываем страницу
		_, err := bp.DiskManager.WritePage(frame.TableName, pageID, frame.Page)
		if err == nil {
			// Успешно записали, снимаем флаг dirty
			frame.IsDirty = false
			delete(bp.DirtyPages, pageID)
		}
	}
}

// ========================== MetaInfo Helper Functions ==========================

// readTableList инициализирует список таблиц
func readTableList(diskManager disk_manager.DiskManager) *disk_manager.TablesList {
	tableList, err := diskManager.ReadTableList()
	if err != nil {
		return nil
	}
	return tableList
}

// readMetaInfo читает метаинформацию таблицы
func readMetaInfo(diskManager disk_manager.DiskManager, tableName string) (*MetaInfo, error) {
	metaData, err := diskManager.ReadMetaFile(tableName)
	if err != nil {
		return nil, err
	}
	pageDirectory, err := diskManager.ReadPageDirectory(tableName)
	if err != nil {
		return nil, err
	}
	dataHeaders, err := diskManager.ReadDataHeaders(tableName)
	if err != nil {
		return nil, err
	}

	return &MetaInfo{
		MetaData:      metaData,
		PageDirectory: pageDirectory,
		DataHeaders:   dataHeaders,
	}, nil
}

// writeMetaInfo записывает метаинформацию таблицы
func writeMetaInfo(diskManager disk_manager.DiskManager, tableName string, metaInfo *MetaInfo) error {
	_, err := diskManager.WriteMetaFile(tableName, metaInfo.MetaData)
	if err != nil {
		return err
	}
	_, err = diskManager.WritePageDirectory(tableName, metaInfo.PageDirectory)
	if err != nil {
		return err
	}
	_, err = diskManager.WriteDataHeaders(tableName, metaInfo.DataHeaders)
	if err != nil {
		return err
	}
	return nil
}
