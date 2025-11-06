package backend

import (
	"custom-database/internal/disk_manager"
	"custom-database/internal/parser/ast"
	"errors"
	"fmt"
	"strconv"
)

func (mb *memoryBackend) insertIntoTable(statement *ast.InsertStatement) error {
	if statement.Values == nil {
		return errors.New("insertIntoTable: values is nil")
	}

	err := mb.insertRowIntoBuffer(statement.Table.Value, statement.Values)
	if err != nil {
		return err
	}

	return nil
}

func (mb *memoryBackend) insertRowIntoBuffer(tableName string, values *[]*ast.Expression) error {
	// Читаем мета-информацию о таблице
	metaInfo, err := mb.bufferBool.ReadMetaInfo(tableName)
	if err != nil {
		return err
	}
	metaData := metaInfo.MetaData
	pageDirectory := metaInfo.PageDirectory
	dataHeaders := metaInfo.DataHeaders

	// Проверяем количество колонок
	if metaData.Header.ColumnCount != uint32(len(*values)) {
		return errors.New("column count mismatch")
	}

	// Конвертируем значения в data cells
	dataRow, err := convertValuesToDataCells(values, metaData.Columns)
	if err != nil {
		return err
	}

	// Ищем страницу для вставки
	var page *disk_manager.Page
	var entryIndex int = -1 // Индекс entry в pageDirectory.Entries для используемой страницы

	// Если страниц нет, создаем новую
	if pageDirectory.Header.PageCount == 0 {
		fmt.Println("Creating new page, pageCount == 0", dataRow[0].Data)

		page, err = mb.bufferBool.AddNewPage(tableName, disk_manager.PageID{PageNumber: pageDirectory.Header.NextPageID, TableName: tableName})
		if err != nil {
			return err
		}

		// Вычисляем начальное свободное место для новой страницы
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
		// Если страницы есть, ищем страницу для вставки

		// Ищем свободное место на странице для вставки
		// Проверяем реальное свободное место на странице после чтения
		for i, entry := range pageDirectory.Entries {
			// Предварительная проверка по FreeSpace (может быть устаревшей)
			if entry.FreeSpace > dataRow.GetSize() {
				// Читаем страницу из буфера
				page, err = mb.bufferBool.GetPage(tableName, disk_manager.PageID{PageNumber: entry.PageID, TableName: tableName})
				if err != nil {
					return err
				}

				// Проверяем реальное свободное место на странице
				// Нужно место для: новый слот (SLOT_SIZE) + данные строки
				requiredSpace := disk_manager.SLOT_SIZE + dataRow.GetSize()
				actualFreeSpace := page.Header.Upper - page.Header.Lower

				if actualFreeSpace >= requiredSpace {
					entryIndex = i
					break
				}

				// Если места недостаточно, продолжаем поиск
				page = nil
			}
		}

		// Если страницу со свободным пространством не нашли, создаем новую
		if page == nil {
			page, err = mb.bufferBool.AddNewPage(tableName, disk_manager.PageID{PageNumber: pageDirectory.Header.NextPageID, TableName: tableName})
			if err != nil {
				return err
			}

			// Вычисляем начальное свободное место для новой страницы
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
		return fmt.Errorf("insertIntoTable: insufficient space on page %d, required: %d, available: %d",
			page.Header.PageID, requiredSpace, availableSpace)
	}

	// Изменяем заголовок страницы
	page.Header.RecordCount++
	page.Header.Lower = page.Header.Lower + disk_manager.SLOT_SIZE
	page.Header.Upper = page.Header.Upper - dataRow.GetSize()

	metaData.Header.NextTupleID++

	// Проверка валидности заголовка страницы
	if page.Header.Upper > disk_manager.PAGE_SIZE || page.Header.Lower > disk_manager.PAGE_SIZE || page.Header.Upper < page.Header.Lower {
		return fmt.Errorf("insertIntoTable: page header is invalid, Upper: %d, Lower: %d, PageID: %d",
			page.Header.Upper, page.Header.Lower, page.Header.PageID)
	}

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

	// Update index
	if mb.accessMethods.CheckIndexExists(tableName) {
		err = mb.accessMethods.InsertTupleID(
			tableName,
			uint32(dataRow[0].Data.(int32)),
			// TODO: SlotNumber == 0 ???
			disk_manager.TupleID{PageID: page.Header.PageID, SlotNumber: uint32(len(page.Slots) - 1)},
		)
		if err != nil {
			return err
		}
	}

	mb.bufferBool.WriteMetaInfo(tableName)

	mb.bufferBool.MarkDirty(tableName, disk_manager.PageID{PageNumber: page.Header.PageID, TableName: tableName})
	mb.bufferBool.Unpin(tableName, disk_manager.PageID{PageNumber: page.Header.PageID, TableName: tableName})

	return nil
}

func convertValuesToDataCells(values *[]*ast.Expression, columns []disk_manager.ColumnInfo) (disk_manager.Row, error) {
	dataCells := make([]disk_manager.DataCell, len(*values))

	for i, value := range *values {
		var data interface{}

		switch columns[i].DataType {
		case disk_manager.INT_32_TYPE:
			v, err := strconv.Atoi(value.Literal.Value)
			if err != nil {
				return nil, err
			}
			data = int32(v)
		case disk_manager.TEXT_TYPE:
			data = value.Literal.Value
		}

		dataCells[i] = disk_manager.DataCell{
			DataType: columns[i].DataType,
			Data:     data,
			IsNull:   false,
		}
	}

	return disk_manager.Row(dataCells), nil
}
