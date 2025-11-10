package backend

import (
	"custom-database/internal/disk_manager"
	"custom-database/internal/parser/ast"
	"custom-database/internal/parser/lex"
	"fmt"
)

func (mb *memoryBackend) createTable(statement *ast.CreateTableStatement) error {
	if statement.Columns == nil {
		return nil
	}

	columns := []disk_manager.ColumnInfo{}
	for _, col := range *statement.Columns {
		var dt disk_manager.DataType

		switch col.Datatype.Value {
		case string(lex.IntKeyword):
			dt = disk_manager.INT_32_TYPE
		case string(lex.TextKeyword):
			dt = disk_manager.TEXT_TYPE
		default:
			return fmt.Errorf("Invalid datatype: %s", col.Datatype.Value)
		}

		columns = append(columns, disk_manager.ColumnInfo{
			ColumnNameLength: uint32(len(col.Name.Value)),
			DataType:         dt,
			IsNullable:       0,
			IsPrimaryKey:     0,
			IsAutoIncrement:  0,
			DefaultValue:     0,
			ColumnName:       col.Name.Value,
		})
	}

	err := mb.bufferBool.CreateTable(statement.Table.Value, columns)
	if err != nil {
		return err
	}
	return nil
}
