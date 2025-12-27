package executors

import (
	"custom-database/internal/buffer_bool"
	"custom-database/internal/disk_manager"
	"custom-database/internal/operator_execution"
	"custom-database/internal/query_planning"
	"fmt"
)

// InsertExecutor выполняет INSERT
type InsertExecutor struct {
	bufferPool buffer_bool.BufferPoolInterface
	plan       *query_planning.InsertPlan
	executed   bool
}

// NewInsertExecutor создает новый InsertExecutor
func NewInsertExecutor(bufferPool buffer_bool.BufferPoolInterface, plan *query_planning.InsertPlan) operator_execution.Executor {
	return &InsertExecutor{
		bufferPool: bufferPool,
		plan:       plan,
		executed:   false,
	}
}

// Init инициализирует executor
func (e *InsertExecutor) Init() error {
	return nil
}

// Next выполняет INSERT и возвращает nil (INSERT не возвращает данные)
func (e *InsertExecutor) Next() (*operator_execution.Tuple, error) {
	if e.executed {
		return nil, nil
	}

	// Читаем метаинформацию таблицы
	metaInfo, err := e.bufferPool.ReadMetaInfo(e.plan.TableName)
	if err != nil {
		return nil, fmt.Errorf("failed to read meta info: %w", err)
	}

	// Вставляем все строки
	for _, rowValues := range e.plan.Values {
		err := e.insertRow(metaInfo, rowValues)
		if err != nil {
			return nil, fmt.Errorf("failed to insert row: %w", err)
		}
	}

	e.executed = true
	return nil, nil
}

// insertRow вставляет одну строку в таблицу
func (e *InsertExecutor) insertRow(metaInfo *buffer_bool.MetaInfo, values []query_planning.Value) error {
	metaData := metaInfo.MetaData
	pageDirectory := metaInfo.PageDirectory
	dataHeaders := metaInfo.DataHeaders

	// Проверяем количество колонок
	if metaData.Header.ColumnCount != uint32(len(values)) {
		return fmt.Errorf("column count mismatch: expected %d, got %d", metaData.Header.ColumnCount, len(values))
	}

	// Преобразуем values в disk_manager.Row (DataCell)
	dataRow, err := e.convertValuesToDataCells(values, metaData.Columns)
	if err != nil {
		return fmt.Errorf("failed to convert values: %w", err)
	}

	// Ищем страницу для вставки
	var page *disk_manager.Page
	var entryIndex int = -1

	// Если страниц нет, создаем новую
	if pageDirectory.Header.PageCount == 0 {
		page, err = e.bufferPool.AddNewPage(e.plan.TableName, disk_manager.PageID{
			PageNumber: pageDirectory.Header.NextPageID,
			TableName:  e.plan.TableName,
		})
		if err != nil {
			return err
		}

		initialFreeSpace := page.Header.Upper - page.Header.Lower

		pageDirectory.Entries = append(pageDirectory.Entries, &disk_manager.PageDirectoryEntry{
			PageID:    page.Header.PageID,
			FreeSpace: initialFreeSpace,
			Flags:     0,
		})
		entryIndex = len(pageDirectory.Entries) - 1
		pageDirectory.Header.PageCount++
		pageDirectory.Header.NextPageID++
		dataHeaders.PagesCount++
	} else {
		// Ищем страницу со свободным местом
		for i, entry := range pageDirectory.Entries {
			if entry.FreeSpace > dataRow.GetSize() {
				page, err = e.bufferPool.GetPage(e.plan.TableName, disk_manager.PageID{
					PageNumber: entry.PageID,
					TableName:  e.plan.TableName,
				})
				if err != nil {
					return err
				}

				requiredSpace := disk_manager.SLOT_SIZE + dataRow.GetSize()
				actualFreeSpace := page.Header.Upper - page.Header.Lower

				if actualFreeSpace >= requiredSpace {
					entryIndex = i
					break
				}

				e.bufferPool.Unpin(e.plan.TableName, disk_manager.PageID{
					PageNumber: entry.PageID,
					TableName:  e.plan.TableName,
				})
				page = nil
			}
		}

		// Если страницу со свободным пространством не нашли, создаем новую
		if page == nil {
			page, err = e.bufferPool.AddNewPage(e.plan.TableName, disk_manager.PageID{
				PageNumber: pageDirectory.Header.NextPageID,
				TableName:  e.plan.TableName,
			})
			if err != nil {
				return err
			}

			initialFreeSpace := page.Header.Upper - page.Header.Lower

			pageDirectory.Entries = append(pageDirectory.Entries, &disk_manager.PageDirectoryEntry{
				PageID:    page.Header.PageID,
				FreeSpace: initialFreeSpace,
				Flags:     0,
			})
			entryIndex = len(pageDirectory.Entries) - 1
			pageDirectory.Header.PageCount++
			pageDirectory.Header.NextPageID++
			dataHeaders.PagesCount++
		}
	}

	// Проверяем, достаточно ли места на странице перед вставкой
	requiredSpace := disk_manager.SLOT_SIZE + dataRow.GetSize()
	availableSpace := page.Header.Upper - page.Header.Lower

	if availableSpace < requiredSpace {
		return fmt.Errorf("insufficient space on page %d, required: %d, available: %d",
			page.Header.PageID, requiredSpace, availableSpace)
	}

	// Изменяем заголовок страницы
	page.Header.RecordCount++
	page.Header.Lower = page.Header.Lower + disk_manager.SLOT_SIZE
	page.Header.Upper = page.Header.Upper - dataRow.GetSize()

	metaData.Header.NextTupleID++

	// Обновляем FreeSpace для правильной страницы
	if entryIndex >= 0 && entryIndex < len(pageDirectory.Entries) {
		pageDirectory.Entries[entryIndex].FreeSpace = page.Header.Upper - page.Header.Lower
	}

	// Изменяем слот
	page.Slots = append(page.Slots, disk_manager.PageSlot{
		Offset: page.Header.Upper,
		Length: dataRow.GetSize(),
		Flags:  0,
	})

	// Добавляем данные строки в страницу
	page.Rows = append(page.Rows, dataRow)

	// Изменяем data headers
	dataHeaders.RecordCount++

	// Помечаем страницу как измененную
	e.bufferPool.MarkDirty(e.plan.TableName, disk_manager.PageID{
		PageNumber: page.Header.PageID,
		TableName:  e.plan.TableName,
	})

	// Записываем метаинформацию
	err = e.bufferPool.WriteMetaInfo(e.plan.TableName)
	if err != nil {
		return fmt.Errorf("failed to write meta info: %w", err)
	}

	e.bufferPool.Unpin(e.plan.TableName, disk_manager.PageID{
		PageNumber: page.Header.PageID,
		TableName:  e.plan.TableName,
	})

	return nil
}

// convertValuesToDataCells преобразует query_planning.Value в disk_manager.DataCell
func (e *InsertExecutor) convertValuesToDataCells(values []query_planning.Value, columns []disk_manager.ColumnInfo) (disk_manager.Row, error) {
	if len(values) != len(columns) {
		return nil, fmt.Errorf("value count mismatch: expected %d, got %d", len(columns), len(values))
	}

	dataCells := make([]disk_manager.DataCell, len(values))

	for i, val := range values {
		var data interface{}

		if val.IsNull {
			dataCells[i] = disk_manager.DataCell{
				DataType: columns[i].DataType,
				Data:     nil,
				IsNull:   true,
			}
			continue
		}

		switch columns[i].DataType {
		case disk_manager.INT_32_TYPE:
			if val.Type != query_planning.ValueInt {
				return nil, fmt.Errorf("type mismatch for column %d: expected INT, got %v", i, val.Type)
			}
			data = val.IntVal
		case disk_manager.TEXT_TYPE:
			if val.Type != query_planning.ValueText {
				return nil, fmt.Errorf("type mismatch for column %d: expected TEXT, got %v", i, val.Type)
			}
			data = val.StrVal
		default:
			return nil, fmt.Errorf("unsupported data type for column %d: %v", i, columns[i].DataType)
		}

		dataCells[i] = disk_manager.DataCell{
			DataType: columns[i].DataType,
			Data:     data,
			IsNull:   false,
		}
	}

	return disk_manager.Row(dataCells), nil
}

// Close освобождает ресурсы
func (e *InsertExecutor) Close() error {
	return nil
}
