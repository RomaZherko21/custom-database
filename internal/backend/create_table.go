package backend

import (
	"custom-database/internal/models"
	"custom-database/internal/parser/ast"
	"custom-database/internal/parser/lex"
	"fmt"
)

func (mb *memoryBackend) createTable(statement *ast.CreateTableStatement) error {
	if statement.Columns == nil {
		return nil
	}

	columns := []models.Column{}
	for _, col := range *statement.Columns {
		var dt models.ColumnType

		switch col.Datatype.Value {
		case string(lex.IntKeyword):
			dt = models.Int32Type
		case string(lex.TextKeyword):
			dt = models.TextType
		default:
			return fmt.Errorf("invalid datatype: %s", col.Datatype.Value)
		}

		columns = append(columns, models.Column{
			Name: col.Name.Value,
			Type: dt,
		})
	}

	err := mb.persistentStorage.CreateTable(statement.Table.Value, columns)
	if err != nil {
		return err
	}

	return nil
}
