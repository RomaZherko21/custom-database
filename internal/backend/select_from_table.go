package backend

import (
	"custom-database/internal/disk_manager"
	"custom-database/internal/parser/ast"
	"errors"
	"fmt"
	"strconv"
)

func (mb *memoryBackend) selectFromTable(statement *ast.SelectStatement) (*Table, error) {
	// Проверяем есть ли индекс
	if mb.accessMethods.CheckIndexExists(statement.Table.Value) && statement.Where != nil {
		return mb.selectRowFromIndex(statement)
	}

	if statement.Where != nil {
		return mb.selectRowFromBuffer(statement)
	}

	return mb.selectRowsFromBuffer(statement.Table.Value)
}

func (mb *memoryBackend) selectRowFromIndex(statement *ast.SelectStatement) (*Table, error) {
	tableName := statement.Table.Value
	searchId, err := strconv.Atoi(statement.Where.Right.Token.Value)
	if err != nil {
		return nil, err
	}
	searchIdUint32 := uint32(searchId)

	// Читаем мета-информацию о таблице
	metaInfo, err := mb.bufferBool.ReadMetaInfo(tableName)
	if err != nil {
		return nil, err
	}

	// Получаем TupleID из индекса
	indexEntry, found := mb.accessMethods.GetTupleID(tableName, searchIdUint32)
	if !found {
		return nil, errors.New("tuple not found")
	}

	// Читаем страницу из буфера
	rows := []disk_manager.Row{}
	page, err := mb.bufferBool.GetPage(tableName, disk_manager.PageID{PageNumber: indexEntry.TupleID.PageID, TableName: tableName})
	if err != nil {
		return nil, err
	}
	rows = append(rows, page.Rows[indexEntry.TupleID.SlotNumber])

	mb.bufferBool.Unpin(tableName, disk_manager.PageID{PageNumber: indexEntry.TupleID.PageID, TableName: tableName})

	return &Table{
		Name:    tableName,
		Columns: convertColumnsToColumns(metaInfo.MetaData.Columns),
		Rows:    convertRowsToRows(rows),
	}, nil
}

func (mb *memoryBackend) selectRowFromBuffer(statement *ast.SelectStatement) (*Table, error) {
	tableName := statement.Table.Value
	searchId, err := strconv.Atoi(statement.Where.Right.Token.Value)
	if err != nil {
		return nil, err
	}
	searchIdUint32 := uint32(searchId)

	// Читаем мета-информацию о таблице
	metaInfo, err := mb.bufferBool.ReadMetaInfo(tableName)
	if err != nil {
		return nil, err
	}

	// Проходимся по всем страницам и ищем строку
	for _, entry := range metaInfo.PageDirectory.Entries {
		page, err := mb.bufferBool.GetPage(tableName, disk_manager.PageID{PageNumber: entry.PageID, TableName: tableName})
		if err != nil {
			return nil, err
		}

		rows := []disk_manager.Row{}
		rows = append(rows, page.Rows...)
		mb.bufferBool.Unpin(tableName, disk_manager.PageID{PageNumber: entry.PageID, TableName: tableName})

		for _, row := range rows {
			if len(row) == 0 {
				continue
			}

			// Пытаемся получить значение из первой колонки
			var value int32
			var ok bool

			// Пробуем разные типы
			if row[0].Data != nil {
				value, ok = row[0].Data.(int32)
				if !ok {
					// Пробуем uint32
					if uintVal, ok2 := row[0].Data.(uint32); ok2 {
						value = int32(uintVal)
						ok = true
					}
				}
			}

			if !ok {
				return nil, fmt.Errorf("failed to convert value to int32, got type: %T, value: %v", row[0].Data, row[0].Data)
			}

			if int32(searchIdUint32) == value {
				convertedRows := convertRowsToRows([]disk_manager.Row{row})
				if convertedRows == nil {
					return nil, fmt.Errorf("failed to convert rows to cells")
				}

				return &Table{
					Name:    tableName,
					Columns: convertColumnsToColumns(metaInfo.MetaData.Columns),
					Rows:    convertedRows,
				}, nil
			}
		}
	}

	return nil, errors.New("row not found")
}

func convertColumnsToColumns(columns []disk_manager.ColumnInfo) []Column {
	result := []Column{}

	for _, column := range columns {
		c := Column{
			Name: column.ColumnName,
		}
		if column.DataType == disk_manager.INT_32_TYPE {
			c.Type = IntType
		}
		if column.DataType == disk_manager.TEXT_TYPE {
			c.Type = TextType
		}
		result = append(result, c)
	}

	return result
}

func convertRowsToRows(rows []disk_manager.Row) [][]Cell {
	result := [][]Cell{}

	for _, row := range rows {
		newRow := []Cell{}
		for _, cell := range row {
			if cell.IsNull {
				newRow = append(newRow, MemoryCell("null"))
			} else if cell.DataType == disk_manager.INT_32_TYPE {
				value, ok := cell.Data.(int32)
				if !ok {
					// Пытаемся конвертировать другие числовые типы
					if uintVal, ok := cell.Data.(uint32); ok {
						value = int32(uintVal)
					} else {
						return nil
					}
				}
				newRow = append(newRow, MemoryCell(strconv.Itoa(int(value))))
			} else if cell.DataType == disk_manager.TEXT_TYPE {
				value, ok := cell.Data.(string)
				if !ok {
					return nil
				}
				newRow = append(newRow, MemoryCell(value))
			}
		}
		result = append(result, newRow)
	}

	fmt.Println("result", result)

	return result
}

func (mb *memoryBackend) selectRowsFromBuffer(tableName string) (*Table, error) {
	metaInfo, err := mb.bufferBool.ReadMetaInfo(tableName)
	if err != nil {
		return nil, err
	}

	rows := []disk_manager.Row{}
	for _, entry := range metaInfo.PageDirectory.Entries {
		page, err := mb.bufferBool.GetPage(tableName, disk_manager.PageID{PageNumber: entry.PageID, TableName: tableName})
		if err != nil {
			return nil, err
		}
		rows = append(rows, page.Rows...)

		mb.bufferBool.Unpin(tableName, disk_manager.PageID{PageNumber: entry.PageID, TableName: tableName})
	}

	convertedRows := convertRowsToRows(rows)
	if convertedRows == nil {
		return nil, fmt.Errorf("failed to convert rows to cells")
	}

	return &Table{
		Name:    tableName,
		Columns: convertColumnsToColumns(metaInfo.MetaData.Columns), Rows: convertedRows,
	}, nil
}
