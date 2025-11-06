package backend

import (
	"custom-database/internal/parser/ast"
	"errors"
)

func (mb *memoryBackend) createIndex(statement *ast.CreateIndexStatement) error {
	if statement.Column == nil {
		return errors.New("column is nil")
	}

	err := mb.accessMethods.CreateIndex(statement.Table.Value, statement.Column.Literal.Value)
	if err != nil {
		return err
	}

	return nil
}

func (mb *memoryBackend) dropIndex(statement *ast.DropIndexStatement) error {
	err := mb.accessMethods.DropIndex(statement.IndexName.Value)
	if err != nil {
		return err
	}
	return nil
}
