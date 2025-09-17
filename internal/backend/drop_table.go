package backend

import "custom-database/internal/parser/ast"

func (mb *memoryBackend) dropTable(statement *ast.DropTableStatement) error {
	err := mb.persistentStorage.DropTable(statement.Table.Value)
	if err != nil {
		return err
	}

	return nil
}
