package backend

import (
	"bytes"
	"custom-database/internal/models"
	"custom-database/internal/parser/ast"
	"encoding/binary"
	"fmt"
	"slices"
)

func (mb *memoryBackend) selectFromTable(statement *ast.SelectStatement) (*models.Table, error) {
	table, err := mb.persistentStorage.Select(statement.Table.Value)
	if err != nil {
		return nil, err
	}

	table.Rows, table.Columns = mb.getOnlySelectedColumns(table.Rows, table.Columns, statement)

	rows, err := mb.convertRowsToCells(table.Rows, table.Columns)
	if err != nil {
		return nil, err
	}

	return &models.Table{
		Name:    table.Name,
		Columns: table.Columns,
		Cells:   rows,
	}, nil
}

func (mb *memoryBackend) getOnlySelectedColumns(allRows [][]interface{}, allColumns []models.Column, statement *ast.SelectStatement) ([][]interface{}, []models.Column) {
	rows := [][]interface{}{}
	columns := []models.Column{}

	if statement.SelectedColumns != nil && len(statement.SelectedColumns) != 0 {
		selectedColumnNames := []string{}
		for _, value := range statement.SelectedColumns {
			selectedColumnNames = append(selectedColumnNames, value.Literal.Value)
		}

		for _, row := range allRows {
			resultRow := []interface{}{}
			for i, column := range allColumns {
				if slices.Contains(selectedColumnNames, column.Name) {
					resultRow = append(resultRow, row[i])
				}
			}

			rows = append(rows, resultRow)
		}
		newColumns := []models.Column{}
		for _, column := range allColumns {
			if slices.Contains(selectedColumnNames, column.Name) {
				newColumns = append(newColumns, column)
			}
		}
		columns = newColumns
	} else {
		return allRows, allColumns
	}

	return rows, columns
}

func (mb *memoryBackend) convertRowsToCells(rows [][]interface{}, columns []models.Column) ([][]models.Cell, error) {
	convertedRows := [][]models.Cell{}

	for _, row := range rows {
		newRow := []models.Cell{}
		for i, cell := range row {
			column := columns[i]
			var memoryCell MemoryCell

			if column.Type == models.Int32Type {
				if cell == nil {
					memoryCell = MemoryCell("null")
				} else {
					buf := new(bytes.Buffer)
					err := binary.Write(buf, binary.BigEndian, int32(cell.(float64)))
					if err != nil {
						return nil, fmt.Errorf("failed to convert int: %w", err)
					}
					memoryCell = MemoryCell(buf.Bytes())
				}
			}

			if column.Type == models.TextType {
				if cell == nil {
					memoryCell = MemoryCell("null")
				} else {
					memoryCell = MemoryCell(cell.(string))
				}
			}

			newRow = append(newRow, memoryCell)
		}
		convertedRows = append(convertedRows, newRow)
	}

	return convertedRows, nil
}
